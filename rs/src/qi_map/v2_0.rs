//! QI force map data (`.jpk-qi-data`) file format version 2.0 reader.

use crate::dataset::{
    properties::{self, Properties, extract_value},
    v2_0 as dataset,
    v2_0::DatasetReader,
};
use derive_more::derive;
use polars::{
    prelude::{self as pl, IntoColumn},
    series::IntoSeries,
};
use rayon::prelude::*;
use std::{
    fs, io,
    path::{Path, PathBuf},
};

type Pixel = crate::image::Pixel<dataset::IndexType>;

const DATASET_DATA_FILE_PROPERTY_VALUE: &str = "spm-quantitative-image-data-file";
const DATASET_TYPE_PROPERTY_VALUE: &str = "quantitative-imaging-map";

enum Index {
    Range {
        min: dataset::IndexType,
        max: dataset::IndexType,
    },
}

impl Index {
    const TYPE_PROPERTY_KEY: &str = "quantitative-imaging-map.indexes.type";
    const MIN_PROPERTY_KEY: &str = "quantitative-imaging-map.indexes.min";
    const MAX_PROPERTY_KEY: &str = "quantitative-imaging-map.indexes.max";

    pub fn from_properties(
        properties: &dataset::properties::Dataset,
    ) -> Result<Self, properties::error::Property> {
        let kind = extract_value!(properties, Self::TYPE_PROPERTY_KEY)?;
        match kind.as_str() {
            "range" => {
                let min =
                    extract_value!(properties, Self::MIN_PROPERTY_KEY, parse dataset::IndexType)?;
                let max =
                    extract_value!(properties, Self::MAX_PROPERTY_KEY, parse dataset::IndexType)?;
                Ok(Index::Range { min, max })
            }
            _ => {
                return Err(properties::error::Property::InvalidValue(format!(
                    "unknown index type {kind}"
                )));
            }
        }
    }
}

enum Numbering {
    LeftToRight,
}

impl Numbering {
    pub fn from_str(input: impl AsRef<str>) -> Option<Self> {
        match input.as_ref() {
            "left-to-right" => Some(Self::LeftToRight),
            _ => None,
        }
    }
}

enum PositionPatternType {
    Grid(Grid),
}

impl PositionPatternType {
    pub fn from_properties(properties: &Properties) -> Result<Self, properties::error::Property> {
        let kind = extract_value!(properties, PositionPattern::TYPE_KEY)?;
        match kind.as_str() {
            "grid-position-pattern" => {
                let grid = Grid::from_properties(properties)?;
                Ok(Self::Grid(grid))
            }
            _ => Err(properties::error::Property::InvalidValue(
                PositionPattern::TYPE_KEY.to_string(),
            )),
        }
    }
}

struct Grid {
    x_center: f64,
    y_center: f64,
    /// fast axis length in `unit`s
    u_length: f64,
    /// slow length in `unit`s
    v_length: f64,
    unit: String,
    /// fast axis pixels
    i_length: u16,
    /// slow axis pixels
    j_length: u16,
}

impl Grid {
    /// `quantitative-imaging-map.position-pattern.grid.xcenter`
    const X_CENTER_KEY: &str = "quantitative-imaging-map.position-pattern.grid.xcenter";
    /// `quantitative-imaging-map.position-pattern.grid.ycenter`
    const Y_CENTER_KEY: &str = "quantitative-imaging-map.position-pattern.grid.ycenter";
    /// `quantitative-imaging-map.position-pattern.grid.ulength`
    const U_LENGTH_KEY: &str = "quantitative-imaging-map.position-pattern.grid.ulength";
    /// `quantitative-imaging-map.position-pattern.grid.vlength`
    const V_LENGTH_KEY: &str = "quantitative-imaging-map.position-pattern.grid.vlength";
    /// `quantitative-imaging-map.position-pattern.grid.theta`
    const THETA_KEY: &str = "quantitative-imaging-map.position-pattern.grid.theta";
    /// `quantitative-imaging-map.position-pattern.grid.reflect`
    const REFLECT_KEY: &str = "quantitative-imaging-map.position-pattern.grid.reflect";
    /// `quantitative-imaging-map.position-pattern.grid.unit.unit`
    const UNIT_KEY: &str = "quantitative-imaging-map.position-pattern.grid.unit.unit";
    /// `quantitative-imaging-map.position-pattern.grid.ilength`
    const I_LENGTH_KEY: &str = "quantitative-imaging-map.position-pattern.grid.ilength";
    /// `quantitative-imaging-map.position-pattern.grid.jlength`
    const J_LENGTH_KEY: &str = "quantitative-imaging-map.position-pattern.grid.jlength";

    pub fn from_properties(properties: &Properties) -> Result<Self, properties::error::Property> {
        let x_center = extract_value!(properties, Self::X_CENTER_KEY, parse f64)?;
        let y_center = extract_value!(properties, Self::Y_CENTER_KEY, parse f64)?;
        let u_length = extract_value!(properties, Self::U_LENGTH_KEY, parse f64)?;
        let v_length = extract_value!(properties, Self::V_LENGTH_KEY, parse f64)?;
        let unit = extract_value!(properties, Self::UNIT_KEY)?.clone();
        let i_length = extract_value!(properties, Self::I_LENGTH_KEY, parse u16)?;
        let j_length = extract_value!(properties, Self::J_LENGTH_KEY, parse u16)?;

        Ok(Self {
            x_center,
            y_center,
            u_length,
            v_length,
            unit,
            i_length,
            j_length,
        })
    }
}

struct PositionPattern {
    numbering: Numbering,
    kind: PositionPatternType,
}

impl PositionPattern {
    const TYPE_KEY: &str = "quantitative-imaging-map.position-pattern.type";
    const NUMBERING_KEY: &str = "quantitative-imaging-map.position-pattern.numbering";

    pub fn from_properties(properties: &Properties) -> Result<Self, properties::error::Property> {
        let numbering = extract_value!(properties, Self::NUMBERING_KEY, from_str Numbering)?;
        let kind = PositionPatternType::from_properties(properties)?;
        Ok(Self { numbering, kind })
    }

    /// # Returns
    /// `None` if pixel coordinate is invalid.
    pub fn pixel_to_index(&self, pixel: &Pixel) -> Option<dataset::IndexType> {
        match &self.kind {
            PositionPatternType::Grid(grid) => {
                if pixel.i >= grid.i_length as dataset::IndexType
                    || pixel.j >= grid.j_length as dataset::IndexType
                {
                    return None;
                }

                Some(pixel.j * grid.i_length as dataset::IndexType + pixel.i)
            }
        }
    }

    /// # Returns
    /// `None` if index is invalid.
    pub fn index_to_pixel(&self, index: dataset::IndexType) -> Option<Pixel> {
        match &self.kind {
            PositionPatternType::Grid(grid) => {
                let max_index =
                    grid.i_length as dataset::IndexType * grid.j_length as dataset::IndexType - 1;
                if index > max_index {
                    return None;
                }
                let j = index / grid.i_length as dataset::IndexType;
                let i = index % grid.i_length as dataset::IndexType;
                return Some(Pixel { i, j });
            }
        }
    }
}

pub struct DatasetInfo {
    index: Index,
    position_pattern: PositionPattern,
}

impl DatasetInfo {
    pub fn from_properties(
        properties: &dataset::properties::Dataset,
    ) -> Result<Self, properties::error::Property> {
        let index = Index::from_properties(&properties)?;
        let position_pattern = PositionPattern::from_properties(&properties)?;
        Ok(Self {
            index,
            position_pattern,
        })
    }
}

#[derive(derive_more::Deref)]
pub struct Reader<R> {
    #[deref]
    inner: dataset::DatasetReader<R>,
    dataset_info: DatasetInfo,
}

impl<R> Reader<R>
where
    R: io::Read + io::Seek,
{
    pub fn new(
        archive: zip::ZipArchive<R>,
    ) -> Result<Self, crate::dataset::error::Error<crate::dataset::error::Dataset>> {
        let reader = DatasetReader::new(archive)?;
        let dataset_info =
            DatasetInfo::from_properties(reader.dataset_properties()).map_err(|err| match err {
                properties::error::Property::NotFound(key) => crate::dataset::error::Error {
                    paths: vec![PathBuf::from(crate::dataset::DATASET_PROPERTIES_FILE_PATH)],
                    error: crate::dataset::error::Dataset::InvalidFormat {
                        cause: format!("property `{key}` not found"),
                    },
                },
                properties::error::Property::InvalidValue(key) => crate::dataset::error::Error {
                    paths: vec![PathBuf::from(crate::dataset::DATASET_PROPERTIES_FILE_PATH)],
                    error: crate::dataset::error::Dataset::InvalidFormat {
                        cause: format!("property `{key}` has an invalid value"),
                    },
                },
            })?;
        Ok(Self {
            inner: reader,
            dataset_info,
        })
    }

    pub fn segment_properties(
        &mut self,
        index: dataset::IndexType,
        segment: dataset::SegmentType,
    ) -> Result<
        dataset::properties::segment::Properties,
        crate::dataset::error::Error<dataset::error::Properties>,
    > {
        self.inner
            .segment_properties(dataset::utils::index_segment_path(index, segment))
    }
}

impl<R> Reader<R> {
    /// # Returns
    /// If the dataset data file property matches the expected value.
    pub fn validate_dataset_data_file(&self) -> bool {
        self.inner
            .dataset_properties()
            .dataset_type()
            .map(|dataset_type| dataset_type == DATASET_DATA_FILE_PROPERTY_VALUE)
            .unwrap_or(false)
    }

    /// # Returns
    /// If the dataset type property matches the expected value.
    pub fn validate_dataset_type(&self) -> bool {
        self.inner
            .dataset_properties()
            .dataset_type()
            .map(|dataset_type| dataset_type == DATASET_TYPE_PROPERTY_VALUE)
            .unwrap_or(false)
    }
}

impl<R> Reader<R>
where
    R: io::Read + io::Seek,
{
    /// Load data from an index's segment's channel.
    ///
    /// # Returns
    /// `Err((associated file path, error))`
    ///
    /// # Notes
    /// + Loads the segment's properties file.
    pub fn load_data_index_segment_channel(
        &mut self,
        segment_path: impl AsRef<Path>,
        channel: impl AsRef<str>,
    ) -> Result<pl::Series, crate::dataset::error::Error<dataset::error::SegmentChannelData>> {
        let properties = self
            .inner
            .segment_properties(&segment_path)
            .map_err(|err| err.map_err(Into::into))?;
        let channel_info = properties
            .channel_info(channel.as_ref())
            .map_err(|err| err.map_err(Into::into))?;
        let data = self
            .inner
            .channel_data(&segment_path, &channel_info)
            .map_err(|err| err.map_err(Into::into))?;
        Ok(pl::Float64Chunked::from_vec(channel.as_ref().into(), data).into_series())
    }

    /// Load all data from an index's segment.
    ///
    /// # Returns
    /// `Err((associated file path, error))`
    ///
    /// # Notes
    /// + Loads the segment's properties file.
    pub fn load_data_index_segment(
        &mut self,
        segment_path: impl AsRef<Path>,
    ) -> Result<pl::DataFrame, crate::dataset::error::Error<dataset::error::CollectionData>> {
        let properties = self
            .inner
            .segment_properties(&segment_path)
            .map_err(|err| {
                err.map_err(|err| dataset::error::SegmentChannelData::SegmentProperties(err).into())
            })?;
        let channels = properties.channel_list().map_err(|err| {
            err.map_err(|err| dataset::error::SegmentChannelData::Property(err).into())
        })?;
        let columns =
            channels
                .into_iter()
                .map(|channel| {
                    let channel_info = properties.channel_info(channel).map_err(|err| {
                        err.map_err(|err| dataset::error::SegmentChannelData::Property(err).into())
                    })?;
                    let data = self
                        .inner
                        .channel_data(&segment_path, &channel_info)
                        .map_err(|err| {
                            err.map_err(|err| {
                                dataset::error::SegmentChannelData::ChannelData(err).into()
                            })
                        })?;
                    Ok(pl::Float64Chunked::from_vec(channel.into(), data).into_column())
                })
                .collect::<Result<
                    Vec<pl::Column>,
                    crate::dataset::error::Error<dataset::error::CollectionData>,
                >>()?;

        let df = pl::DataFrame::new(columns).map_err(|err| crate::dataset::error::Error {
            paths: vec![segment_path.as_ref().to_path_buf()],
            error: err.into(),
        })?;
        Ok(df)
    }

    pub fn load_data_all(&mut self) -> Result<pl::DataFrame, ()> {
        let indices = match self.dataset_info.index {
            Index::Range { min, max } => min..max + 1,
        };

        todo!();
    }
}

mod error {}

// impl<R> super::QIMapReader for Reader<R>
// where
//     R: io::Read + io::Seek,
// {
//     fn query_data(&mut self, query: &super::DataQuery) -> Result<super::Data, super::QueryError> {
//         let indices = self._data_query_indices(query)?;
//         let mut data_idx = Vec::with_capacity(indices.len());
//         for index in indices {
//             let index_data = utils::index_data(&mut self.archive, index)?;
//             let segments = match &query.segment {
//                 super::SegmentQuery::All => (0..index_data.segment_count()).collect::<Vec<_>>(),
//                 super::SegmentQuery::Indices(indices) => indices.clone(),
//             };

//             for segment in segments {
//                 let segment_properties =
//                     utils::segment_properties(&mut self.archive, index, segment)?;
//                 let segment_data = utils::segment_data(&segment_properties, index)?;
//                 let channels = match &query.channel {
//                     super::ChannelQuery::All => segment_data.channels().clone(),
//                     super::ChannelQuery::Include(channels) => {
//                         let mut channels = channels.clone();
//                         channels.retain(|channel| segment_data.channels().contains(channel));
//                         channels
//                     }
//                 };

//                 for channel in channels {
//                     let channel_data =
//                         utils::channel_data(&segment_properties, &channel, index, segment)?;

//                     data_idx.push((
//                         super::DataIndex {
//                             index,
//                             segment,
//                             channel,
//                         },
//                         index,
//                         channel_data.shared_data_index(),
//                         channel_data.file_path().clone(),
//                     ))
//                 }
//             }
//         }

//         let mut data = Vec::with_capacity(data_idx.len());
//         for (idx, index, shared_data_index, file_path) in data_idx {
//             let lcd_info = &self.lcd_info[shared_data_index];
//             let data_file_path = {
//                 let path = utils::index_segment_path(index, idx.segment);
//                 let path = format!("{}/{}", path.to_string_lossy(), file_path.to_string_lossy());
//                 PathBuf::from(path)
//             };

//             let mut data_file = self.archive.by_path(&data_file_path).map_err(|error| {
//                 super::QueryError::ZipFile {
//                     path: data_file_path.clone(),
//                     error,
//                 }
//             })?;
//             let mut raw_data = Vec::with_capacity(data_file.size() as usize);
//             data_file
//                 .read_to_end(&mut raw_data)
//                 .map_err(|err| super::QueryError::ZipFile {
//                     path: data_file_path.clone(),
//                     error: zip::result::ZipError::Io(err),
//                 })?;

//             let ch_data = lcd_info.convert_data(&raw_data).map_err(|_err| {
//                 super::QueryError::InvalidData {
//                     path: data_file_path.clone(),
//                 }
//             })?;

//             data.push((idx, ch_data));
//         }

//         let (idx, data) = data.into_iter().unzip();
//         let data = super::Data::new(idx, data).unwrap();
//         Ok(data)
//     }

//     fn query_metadata(
//         &mut self,
//         query: &super::MetadataQuery,
//     ) -> Result<super::Metadata, super::QueryError> {
//         match query {
//             super::MetadataQuery::All => self.metadata_all(),
//             super::MetadataQuery::Dataset => self.metadata_dataset(),
//             super::MetadataQuery::SharedData => self.metadata_shared(),
//             super::MetadataQuery::Index(query) => match query {
//                 super::IndexQuery::All => todo!("Reader::query_metadata(IndexQuery::All)"),
//                 super::IndexQuery::Index(index) => {
//                     todo!("Reader::query_metadata(IndexQuery::Index)")
//                 }
//                 super::IndexQuery::PixelRect(rect) => {
//                     todo!("Reader::query_metadata(IndexQuery::PixelRect)")
//                 }
//                 super::IndexQuery::Pixel(pixel) => self.metadata_index_pixel(pixel),
//             },
//             super::MetadataQuery::Segment { index, segment } => {
//                 todo!("Reader::query_metadata(SegmentQuery)")
//             }
//         }
//     }
// }

// impl<R> Reader<R>
// where
//     R: io::Read + io::Seek,
// {
//     fn _data_query_indices(
//         &mut self,
//         query: &super::DataQuery,
//     ) -> Result<Vec<IndexType>, super::QueryError> {
//         match &query.index {
//             super::IndexQuery::All => match self.dataset_info.index {
//                 Index::Range { min, max } => Ok((min..=max).collect::<Vec<_>>()),
//             },

//             super::IndexQuery::Index(index) => Ok(vec![*index]),

//             super::IndexQuery::PixelRect(rect) => rect
//                 .iter()
//                 .map(|pixel| {
//                     self.dataset_info
//                         .position_pattern
//                         .pixel_to_index(&pixel)
//                         .ok_or(super::QueryError::OutOfBounds(pixel))
//                 })
//                 .collect::<Result<Vec<_>, _>>(),

//             super::IndexQuery::Pixel(pixel) => {
//                 let idx = self
//                     .dataset_info
//                     .position_pattern
//                     .pixel_to_index(pixel)
//                     .ok_or(super::QueryError::OutOfBounds(pixel.clone()))?;
//                 Ok(vec![idx])
//             }
//         }
//     }
// }

// impl<R> Reader<R>
// where
//     R: io::Read + io::Seek,
// {
//     fn metadata_all(&mut self) -> Result<super::Metadata, super::QueryError> {
//         let mut metadata = super::Metadata::with_capacity(self.archive.len() / 2);
//         for idx in 0..self.archive.len() {
//             let mut file = self
//                 .archive
//                 .by_index(idx)
//                 .map_err(|err| super::QueryError::Zip(err))?;

//             let index =
//                 metadata_index_from_file_path(file.name(), &self.dataset_info.position_pattern)
//                     .map_err(|err| super::QueryError::ZipFile {
//                         path: PathBuf::from(file.name()),
//                         error: err,
//                     })?;
//             let Some(index) = index else {
//                 continue;
//             };

//             let properties = super::Properties::new(&mut file).map_err(|_| {
//                 super::QueryError::InvalidFormat {
//                     path: PathBuf::from(file.name()),
//                     cause: "file could not be read as properties".to_string(),
//                 }
//             })?;

//             metadata.insert(index, properties);
//         }

//         Ok(metadata)
//     }

//     fn metadata_dataset(&mut self) -> Result<super::Metadata, super::QueryError> {
//         let mut properties = self
//             .archive
//             .by_path(utils::DATASET_PROPERTIES_FILE)
//             .map_err(|error| super::QueryError::ZipFile {
//                 path: PathBuf::from(utils::DATASET_PROPERTIES_FILE),
//                 error,
//             })?;

//         let properties =
//             Properties::new(&mut properties).map_err(|_| super::QueryError::InvalidFormat {
//                 path: PathBuf::from(utils::DATASET_PROPERTIES_FILE),
//                 cause: "file could not be read as properties".to_string(),
//             })?;

//         let indices = vec![super::MetadataIndex::Dataset];
//         let data = vec![properties];
//         Ok(super::Metadata::from_parts(indices, data).unwrap())
//     }

//     fn metadata_shared(&mut self) -> Result<super::Metadata, super::QueryError> {
//         let data_path = utils::shared_data_properties_path();
//         let mut properties =
//             self.archive
//                 .by_path(&data_path)
//                 .map_err(|error| super::QueryError::ZipFile {
//                     path: data_path.clone(),
//                     error,
//                 })?;

//         let properties =
//             Properties::new(&mut properties).map_err(|_| super::QueryError::InvalidFormat {
//                 path: data_path.clone(),
//                 cause: "file could not be read as properties".to_string(),
//             })?;

//         let indices = vec![super::MetadataIndex::SharedData];
//         let data = vec![properties];
//         Ok(super::Metadata::from_parts(indices, data).unwrap())
//     }

//     fn metadata_index_pixel(
//         &mut self,
//         pixel: &super::Pixel,
//     ) -> Result<super::Metadata, super::QueryError> {
//         let Some(index) = self.dataset_info.position_pattern.pixel_to_index(pixel) else {
//             return Err(super::QueryError::OutOfBounds(pixel.clone()));
//         };
//         let data_path = utils::index_properties_path(index);
//         let mut properties =
//             self.archive
//                 .by_path(&data_path)
//                 .map_err(|error| super::QueryError::ZipFile {
//                     path: data_path.clone(),
//                     error,
//                 })?;
//         let properties =
//             Properties::new(&mut properties).map_err(|_| super::QueryError::InvalidFormat {
//                 path: data_path.clone(),
//                 cause: "file could not be read as properties".to_string(),
//             })?;

//         let idx = vec![super::MetadataIndex::Index(index)];
//         let data = vec![properties];
//         Ok(super::Metadata::from_parts(idx, data).unwrap())
//     }
// }

// fn metadata_index_from_file_path(
//     filename: &str,
//     position_pattern: &PositionPattern,
// ) -> Result<Option<super::MetadataIndex>, zip::result::ZipError> {
//     const INDEX_PREFIX: &str = "index/";
//     const SEGMENT_PREFIX: &str = "segments/";

//     if filename == super::DATASET_PROPERTIES_FILE_PATH {
//         return Ok(Some(super::MetadataIndex::Dataset));
//     } else if filename == format!("{}/{}", SHARED_DATA_DIR, utils::SHARED_DATA_PROPERTIES_FILE) {
//         return Ok(Some(super::MetadataIndex::SharedData));
//     } else if filename.ends_with(utils::SEGMENT_PROPERTIES_FILE) {
//         let Some((index_str, _)) = filename[INDEX_PREFIX.len()..].split_once("/") else {
//             return Err(zip::result::ZipError::InvalidArchive(
//                 std::borrow::Cow::Borrowed("invalid file path"),
//             ));
//         };

//         let Ok(index) = index_str.parse::<IndexType>() else {
//             return Err(zip::result::ZipError::InvalidArchive(
//                 std::borrow::Cow::Borrowed("invalid file path"),
//             ));
//         };

//         let Some((_, segment_str)) =
//             filename[..filename.len() - utils::SEGMENT_PROPERTIES_FILE.len() - 1].rsplit_once("/")
//         else {
//             return Err(zip::result::ZipError::InvalidArchive(
//                 std::borrow::Cow::Borrowed("invalid file path"),
//             ));
//         };

//         let Ok(segment) = segment_str.parse::<SegmentType>() else {
//             return Err(zip::result::ZipError::InvalidArchive(
//                 std::borrow::Cow::Borrowed("invalid file path"),
//             ));
//         };

//         return Ok(Some(super::MetadataIndex::Segment { index, segment }));
//     } else if filename.starts_with(INDEX_PREFIX)
//         && filename.ends_with(&format!("/{}", utils::INDEX_PROPERTIES_FILE))
//     {
//         let Some((index_str, _)) = filename[INDEX_PREFIX.len()..].split_once("/") else {
//             return Err(zip::result::ZipError::InvalidArchive(
//                 std::borrow::Cow::Borrowed("invalid file path"),
//             ));
//         };

//         let Ok(index) = index_str.parse::<IndexType>() else {
//             return Err(zip::result::ZipError::InvalidArchive(
//                 std::borrow::Cow::Borrowed("invalid file path"),
//             ));
//         };

//         return Ok(Some(super::MetadataIndex::Index(index)));
//     } else {
//         return Ok(None);
//     }
// }

/// JPK reader optimized for files.
/// Allows parallel reading of datasets, where as [`Reader`] must read things in series.
#[derive(derive_more::Deref)]
pub struct FileReader {
    #[deref]
    inner: Reader<fs::File>,
    file_path: PathBuf,
}

impl FileReader {
    pub fn new(
        path: impl Into<PathBuf>,
    ) -> Result<Self, crate::dataset::error::Error<crate::dataset::error::Dataset>> {
        let path = path.into();
        let file = fs::File::open(&path)
            .map_err(|err| match err.kind() {
                io::ErrorKind::NotFound => zip::result::ZipError::FileNotFound,
                _ => zip::result::ZipError::Io(err),
            })
            .map_err(|err| crate::dataset::error::Error {
                paths: vec![path.clone()],
                error: crate::dataset::error::Dataset::OpenArchive(err),
            })?;
        let archive = zip::ZipArchive::new(file).map_err(|err| crate::dataset::error::Error {
            paths: vec![path.clone()],
            error: crate::dataset::error::Dataset::OpenArchive(err),
        })?;
        let inner = Reader::new(archive)?;
        Ok(Self {
            inner,
            file_path: path,
        })
    }

    /// Create a new file reader with an archive that has already been loaded.
    ///
    /// # Safety
    /// It is left to the user to ensure that `path` and `archive` are compatible.
    pub unsafe fn new_with_archive(
        path: impl Into<PathBuf>,
        archive: zip::ZipArchive<fs::File>,
    ) -> Result<Self, crate::dataset::error::Error<crate::dataset::error::Dataset>> {
        let path = path.into();
        let inner = Reader::new(archive)?;
        Ok(Self {
            inner,
            file_path: path,
        })
    }
}

// impl super::QIMapReader for FileReader {
//     fn query_data(&mut self, query: &super::DataQuery) -> Result<super::Data, super::QueryError> {
//         let indices = self.inner._data_query_indices(query)?;
//         let data_idx = indices
//             .into_par_iter()
//             .map_init(
//                 {
//                     let metadata = self.inner.archive.metadata();
//                     let file_path = &self.file_path;
//                     move || {
//                         let file = fs::File::open(file_path).expect("could not open file");
//                         unsafe { zip::ZipArchive::unsafe_new_with_metadata(file, metadata.clone()) }
//                     }
//                 },
//                 |archive, idx| {
//                     let index_data = utils::index_data(archive, idx)?;
//                     let segments = match &query.segment {
//                         super::SegmentQuery::All => {
//                             (0..index_data.segment_count()).collect::<Vec<_>>()
//                         }
//                         super::SegmentQuery::Indices(indices) => indices.clone(),
//                     };

//                     let idx = segments
//                         .into_iter()
//                         .map(|segment| (idx, segment))
//                         .collect::<Vec<_>>();
//                     Ok(idx)
//                 },
//             )
//             .collect::<Result<Vec<_>, _>>()?;
//         let data_idx = data_idx.into_iter().flatten().collect::<Vec<_>>();

//         let data_idx = data_idx
//             .into_par_iter()
//             .map_init(
//                 {
//                     let metadata = self.inner.archive.metadata();
//                     let file_path = &self.file_path;
//                     move || {
//                         let file = fs::File::open(file_path).expect("could not open file");
//                         unsafe { zip::ZipArchive::unsafe_new_with_metadata(file, metadata.clone()) }
//                     }
//                 },
//                 |archive, (idx, segment)| {
//                     let segment_properties = utils::segment_properties(archive, idx, segment)?;
//                     let segment_data = utils::segment_data(&segment_properties, idx)?;
//                     let channels = match &query.channel {
//                         super::ChannelQuery::All => segment_data.channels().clone(),
//                         super::ChannelQuery::Include(channels) => {
//                             let mut channels = channels.clone();
//                             channels.retain(|channel| segment_data.channels().contains(channel));
//                             channels
//                         }
//                     };

//                     let idx = channels
//                         .into_iter()
//                         .map(|channel| {
//                             let channel_data =
//                                 utils::channel_data(&segment_properties, &channel, idx, segment)?;

//                             Ok(((idx, segment, channel), channel_data))
//                         })
//                         .collect::<Result<Vec<_>, _>>()?;

//                     Ok(idx)
//                 },
//             )
//             .collect::<Result<Vec<_>, _>>()?;

//         let data_idx = data_idx
//             .into_iter()
//             .flatten()
//             .map(|((index, segment, channel), channel_data)| {
//                 (
//                     super::DataIndex {
//                         index,
//                         segment,
//                         channel,
//                     },
//                     channel_data.shared_data_index(),
//                     channel_data.file_path().clone(),
//                 )
//             })
//             .collect::<Vec<_>>();

//         let raw_data = data_idx
//             .into_par_iter()
//             .map_init(
//                 {
//                     let metadata = self.inner.archive.metadata();
//                     let file_path = &self.file_path;
//                     move || {
//                         let file = fs::File::open(file_path).expect("could not open file");
//                         unsafe { zip::ZipArchive::unsafe_new_with_metadata(file, metadata.clone()) }
//                     }
//                 },
//                 |archive, (idx, shared_data_index, file_path)| {
//                     let data_file_path = {
//                         let path = utils::index_segment_path(idx.index, idx.segment);
//                         let path =
//                             format!("{}/{}", path.to_string_lossy(), file_path.to_string_lossy());
//                         PathBuf::from(path)
//                     };

//                     let mut data_file = archive.by_path(&data_file_path).map_err(|error| {
//                         super::QueryError::ZipFile {
//                             path: data_file_path.clone(),
//                             error,
//                         }
//                     })?;
//                     let mut raw_data = Vec::with_capacity(data_file.size() as usize);
//                     data_file.read_to_end(&mut raw_data).map_err(|err| {
//                         super::QueryError::ZipFile {
//                             path: data_file_path.clone(),
//                             error: zip::result::ZipError::Io(err),
//                         }
//                     })?;

//                     Ok((idx, raw_data, data_file_path, shared_data_index))
//                 },
//             )
//             .collect::<Result<Vec<_>, _>>()?;

//         let lcd_info = &self.inner.lcd_info;
//         let data = raw_data
//             .into_par_iter()
//             .map_with(
//                 lcd_info,
//                 |lcd_info, (idx, raw_data, data_file_path, shared_data_index)| {
//                     let lcd_info = &lcd_info[shared_data_index];
//                     let ch_data = lcd_info.convert_data(&raw_data).map_err(|_err| {
//                         super::QueryError::InvalidData {
//                             path: data_file_path.clone(),
//                         }
//                     })?;

//                     Ok((idx, ch_data))
//                 },
//             )
//             .collect::<Result<Vec<_>, _>>()?;

//         let (idx, data) = data.into_iter().unzip();
//         let data = super::Data::new(idx, data).unwrap();
//         Ok(data)
//     }

//     fn query_metadata(
//         &mut self,
//         query: &super::MetadataQuery,
//     ) -> Result<super::Metadata, super::QueryError> {
//         match query {
//             super::MetadataQuery::All => self.metadata_all(),
//             super::MetadataQuery::Dataset => self.inner.metadata_dataset(),
//             super::MetadataQuery::SharedData => self.inner.metadata_shared(),
//             super::MetadataQuery::Index(index_query) => match index_query {
//                 super::IndexQuery::All => todo!("FileReader::query_metadata(IndexQuery::All)"),
//                 super::IndexQuery::Index(index) => {
//                     todo!("FileReader::query_metadata(IndexQuery::Index)")
//                 }
//                 super::IndexQuery::PixelRect(pixel_rect) => {
//                     todo!("FileReader::query_metadata(IndexQuery::PixelRect)")
//                 }
//                 super::IndexQuery::Pixel(pixel) => self.inner.metadata_index_pixel(pixel),
//             },
//             super::MetadataQuery::Segment { index, segment } => {
//                 todo!("FileReader::query_metadata(SegmentQuery)")
//             }
//         }
//     }
// }

// impl FileReader {
//     fn metadata_all(&mut self) -> Result<super::Metadata, super::QueryError> {
//         let properties = (0..self.inner.archive.len())
//             .into_par_iter()
//             .map_init(
//                 {
//                     let metadata = self.inner.archive.metadata();
//                     let file_path = &self.file_path;
//                     move || {
//                         let file = fs::File::open(file_path).expect("could not open file");
//                         unsafe { zip::ZipArchive::unsafe_new_with_metadata(file, metadata.clone()) }
//                     }
//                 },
//                 |archive, idx| {
//                     let mut file = archive
//                         .by_index(idx)
//                         .map_err(|err| super::QueryError::Zip(err))?;

//                     metadata_index_from_file_path(
//                         file.name(),
//                         &self.inner.dataset_info.position_pattern,
//                     )
//                     .map_err(|err| super::QueryError::ZipFile {
//                         path: PathBuf::from(file.name()),
//                         error: err,
//                     })
//                     .map(|maybe_index| {
//                         maybe_index
//                             .map(|index| {
//                                 super::Properties::new(&mut file)
//                                     .map(|property| (index, property))
//                                     .map_err(|_| super::QueryError::InvalidFormat {
//                                         path: PathBuf::from(file.name()),
//                                         cause: "file could not be read as properties".to_string(),
//                                     })
//                             })
//                             .transpose()
//                     })
//                     .flatten()
//                 },
//             )
//             .collect::<Result<Vec<_>, _>>()?;

//         let (indices, data) = properties
//             .into_iter()
//             .filter_map(|index| index)
//             .unzip::<_, _, Vec<_>, Vec<_>>();

//         Ok(super::Metadata::from_parts(indices, data).expect("indices and data are compatible"))
//     }
// }

#[derive(Clone, Copy, Debug)]
enum DataFileFormat {
    Raw,
}

impl DataFileFormat {
    pub fn from_str(input: impl AsRef<str>) -> Option<Self> {
        match input.as_ref() {
            "raw" => Some(Self::Raw),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub enum DataError {
    Zip {
        path: PathBuf,
        error: zip::result::ZipError,
    },

    /// The file at the given path had an invalid format.
    InvalidFormat { path: PathBuf, cause: String },

    /// A channel with the given name does not exist.
    ChannelNotFound(String),

    /// The data file at the given path contained invalid data.
    InvalidData { path: PathBuf },
}

pub mod index {
    use crate::dataset::{properties as dataset_properties, v2_0::SegmentType};

    #[derive(derive_more::Deref)]
    pub struct IndexProperties {
        inner: dataset_properties::Properties,
    }

    pub struct Info {
        segment_count: SegmentType,
    }

    impl Info {
        const SEGMENT_COUNT_KEY: &str = "quantitative-imaging-series.force-segments.count";

        pub fn from_properties(
            properties: &dataset_properties::Properties,
        ) -> Result<Self, dataset_properties::error::Property> {
            let segment_count = dataset_properties::extract_value!(
                properties,
                Self::SEGMENT_COUNT_KEY,
                parse SegmentType
            )?;

            Ok(Self { segment_count })
        }
    }

    impl Info {
        pub fn segment_count(&self) -> SegmentType {
            self.segment_count
        }
    }
}

// mod utils {
//     use crate::dataset::{properties as dataset_properties, v2_0 as dataset};
//     use std::{fmt, io};
//     use zip::ZipArchive;

//     pub fn index_data<R>(
//         archive: &mut ZipArchive<R>,
//         index: dataset::IndexType,
//     ) -> Result<super::index::Info, dataset::error::Properties>
//     where
//         R: io::Read + io::Seek,
//     {
//         let index_properties_path = dataset::utils::index_properties_path(index);
//         let mut file = archive.by_path(&index_properties_path)?;
//         let properties = dataset_properties::Properties::new(&mut file)?;

//         super::index::Info::from_properties(&properties)
//     }

//     pub fn segment_properties<R>(
//         archive: &mut zip::ZipArchive<R>,
//         index: dataset::IndexType,
//         segment: dataset::SegmentType,
//     ) -> Result<dataset::properties::segment::Properties, dataset::error::Properties>
//     where
//         R: io::Read + io::Seek,
//     {
//         let segment_properties_path = dataset::utils::index_segment_properties_path(index, segment);
//         let mut properties =
//             archive
//                 .by_path(&segment_properties_path)
//                ?;
//         let properties = properties::Properties::new(&mut properties)?;

//         Ok( { inner: properties })
//     }

//     /// # Notes
//     /// + `index` only used for error reporting.
//     pub fn segment_data(
//         properties: &super::SegmentProperties,
//         index: IndexType,
//     ) -> Result<super::segment_data::SegmentData, super::super::QueryError> {
//         use super::{super::QueryError, PropertyError, segment_data};

//         segment_data::SegmentData::from(properties).map_err(|err| match err {
//             PropertyError::NotFound(key) => QueryError::InvalidFormat {
//                 path: index_properties_path(index),
//                 cause: format!("property `{key}` not found"),
//             },
//             PropertyError::InvalidValue(key) => QueryError::InvalidFormat {
//                 path: index_properties_path(index),
//                 cause: format!("invalid value for `{key}`"),
//             },
//         })
//     }

//     /// # Notes
//     /// + `index` and `segment` only used for error reporting.
//     pub fn channel_data(
//         segment_properties: &super::SegmentProperties,
//         channel: impl fmt::Display,
//         index: IndexType,
//         segment: SegmentType,
//     ) -> Result<super::channel_data::ChannelData, super::super::QueryError> {
//         use super::{super::QueryError, PropertyError, channel_data};

//         channel_data::ChannelData::from(segment_properties, &channel).map_err(|err| match err {
//             PropertyError::NotFound(key) => QueryError::InvalidFormat {
//                 path: index_segment_properties_path(index, segment),
//                 cause: format!("property `{key}` not found"),
//             },
//             PropertyError::InvalidValue(key) => QueryError::InvalidFormat {
//                 path: index_segment_properties_path(index, segment),
//                 cause: format!("invalid value of `{key}`"),
//             },
//         })
//     }
// }
