use super::{IndexType, SegmentType, Value};
use crate::{
    properties::{self, Properties, PropertyError},
    qi_map::v2_0::utils::SHARED_DATA_DIR,
};
use rayon::prelude::*;
use std::{
    fmt,
    fs::{self, File},
    io::{self, Read},
    path::PathBuf,
};

pub mod lcd_info;

const PROPERTIES_DATA_FILE_KEY: &str = "jpk-data-file";
const PROPERTIES_DATA_FILE_VALUE: &str = "spm-quantitative-image-data-file";
const PROPERTIES_DATA_TYPE_KEY: &str = "type";
const PROPERTIES_DATA_TYPE_VALUE: &str = "quantitative-imaging-map";

#[derive(derive_more::Deref, Debug)]
struct IndexProperties {
    inner: Properties,
}

#[derive(derive_more::Deref, Debug)]
struct SegmentProperties {
    inner: Properties,
}

impl SegmentProperties {
    /// `channel.{channel}.data.file.name`
    pub fn channel_data_file_name_key(channel: impl fmt::Display) -> String {
        format!("channel.{channel}.data.file.name")
    }

    /// `channel.{channel}.data.file.format`
    pub fn channel_data_file_format_key(channel: impl fmt::Display) -> String {
        format!("channel.{channel}.data.file.format")
    }

    /// `channel.{channel}.data.num-points`
    pub fn channel_data_num_points_key(channel: impl fmt::Display) -> String {
        format!("channel.{channel}.data.num-points")
    }

    /// `channel.{channel}.lcd-info.*`
    pub fn channel_shared_data_index_key(channel: impl fmt::Display) -> String {
        format!("channel.{channel}.lcd-info.*")
    }
}

#[derive(derive_more::Deref)]
pub struct SharedData {
    inner: Properties,
}

impl SharedData {
    const LCD_INFOS_COUNT_KEY: &str = "lcd-infos.count";

    /// `lcd-info.{index}.encoder.type`
    pub fn lcd_info_encoder_type_key(index: usize) -> String {
        format!("lcd-info.{index}.encoder.type")
    }

    /// `lcd-info.{index}.encoder.scaling.unit.unit``
    pub fn lcd_info_encoder_unit_key(index: usize) -> String {
        format!("lcd-info.{index}.encoder.scaling.unit.unit")
    }

    /// `lcd-info.{index}.encoder.scaling.type`
    pub fn lcd_info_encoder_scaling_type_key(index: usize) -> String {
        format!("lcd-info.{index}.encoder.scaling.type")
    }

    /// `lcd-info.{index}.encoder.scaling.style`
    pub fn lcd_info_encoder_scaling_style_key(index: usize) -> String {
        format!("lcd-info.{index}.encoder.scaling.style")
    }

    /// `lcd-info.{index}.encoder.scaling.offset`
    pub fn lcd_info_encoder_scaling_offset_key(index: usize) -> String {
        format!("lcd-info.{index}.encoder.scaling.offset")
    }

    /// `lcd-info.{index}.encoder.scaling.multiplier`
    pub fn lcd_info_encoder_scaling_multiplier_key(index: usize) -> String {
        format!("lcd-info.{index}.encoder.scaling.multiplier")
    }
}

/// JPK reader optimized for files.
/// Allows parallel reading of datasets, where as [`Reader`] must read things in series.
pub struct FileReader {
    inner: Reader<fs::File>,
    file_path: PathBuf,
}

impl FileReader {
    pub fn new(path: impl Into<PathBuf>) -> Result<Self, super::Error> {
        let path = path.into();
        let file = fs::File::open(&path).map_err(|err| match err.kind() {
            io::ErrorKind::NotFound => zip::result::ZipError::FileNotFound,
            _ => zip::result::ZipError::Io(err),
        })?;
        let archive = zip::ZipArchive::new(file)?;
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
    ) -> Result<Self, super::Error> {
        let path = path.into();
        let inner = Reader::new(archive)?;
        Ok(Self {
            inner,
            file_path: path,
        })
    }
}

impl super::QIMapReader for FileReader {
    fn query_data(&mut self, query: &super::DataQuery) -> Result<super::Data, super::QueryError> {
        let indices = self.inner._data_query_indices(query)?;
        let data_idx = indices
            .into_par_iter()
            .map_init(
                {
                    let metadata = self.inner.archive.metadata();
                    let file_path = &self.file_path;
                    move || {
                        let file = fs::File::open(file_path).expect("could not open file");
                        unsafe { zip::ZipArchive::unsafe_new_with_metadata(file, metadata.clone()) }
                    }
                },
                |archive, idx| {
                    let index_data = utils::index_data(archive, idx)?;
                    let segments = match &query.segment {
                        super::SegmentQuery::All => {
                            (0..index_data.segment_count()).collect::<Vec<_>>()
                        }
                        super::SegmentQuery::Indices(indices) => indices.clone(),
                    };

                    let idx = segments
                        .into_iter()
                        .map(|segment| (idx, segment))
                        .collect::<Vec<_>>();
                    Ok(idx)
                },
            )
            .collect::<Result<Vec<_>, _>>()?;
        let data_idx = data_idx.into_iter().flatten().collect::<Vec<_>>();

        let data_idx = data_idx
            .into_par_iter()
            .map_init(
                {
                    let metadata = self.inner.archive.metadata();
                    let file_path = &self.file_path;
                    move || {
                        let file = fs::File::open(file_path).expect("could not open file");
                        unsafe { zip::ZipArchive::unsafe_new_with_metadata(file, metadata.clone()) }
                    }
                },
                |archive, (idx, segment)| {
                    let segment_properties = utils::segment_properties(archive, idx, segment)?;
                    let segment_data = utils::segment_data(&segment_properties, idx)?;
                    let channels = match &query.channel {
                        super::ChannelQuery::All => segment_data.channels().clone(),
                        super::ChannelQuery::Include(channels) => {
                            let mut channels = channels.clone();
                            channels.retain(|channel| segment_data.channels().contains(channel));
                            channels
                        }
                    };

                    let idx = channels
                        .into_iter()
                        .map(|channel| {
                            let channel_data =
                                utils::channel_data(&segment_properties, &channel, idx, segment)?;

                            Ok(((idx, segment, channel), channel_data))
                        })
                        .collect::<Result<Vec<_>, _>>()?;

                    Ok(idx)
                },
            )
            .collect::<Result<Vec<_>, _>>()?;

        let data_idx = data_idx
            .into_iter()
            .flatten()
            .map(|((index, segment, channel), channel_data)| {
                (
                    super::DataIndex {
                        index,
                        segment,
                        channel,
                    },
                    channel_data.shared_data_index(),
                    channel_data.file_path().clone(),
                )
            })
            .collect::<Vec<_>>();

        let raw_data = data_idx
            .into_par_iter()
            .map_init(
                {
                    let metadata = self.inner.archive.metadata();
                    let file_path = &self.file_path;
                    move || {
                        let file = fs::File::open(file_path).expect("could not open file");
                        unsafe { zip::ZipArchive::unsafe_new_with_metadata(file, metadata.clone()) }
                    }
                },
                |archive, (idx, shared_data_index, file_path)| {
                    let data_file_path = {
                        let path = utils::index_segment_path(idx.index, idx.segment);
                        let path =
                            format!("{}/{}", path.to_string_lossy(), file_path.to_string_lossy());
                        PathBuf::from(path)
                    };

                    let mut data_file = archive.by_path(&data_file_path).map_err(|error| {
                        super::QueryError::ZipFile {
                            path: data_file_path.clone(),
                            error,
                        }
                    })?;
                    let mut raw_data = Vec::with_capacity(data_file.size() as usize);
                    data_file.read_to_end(&mut raw_data).map_err(|err| {
                        super::QueryError::ZipFile {
                            path: data_file_path.clone(),
                            error: zip::result::ZipError::Io(err),
                        }
                    })?;

                    Ok((idx, raw_data, data_file_path, shared_data_index))
                },
            )
            .collect::<Result<Vec<_>, _>>()?;

        let lcd_info = &self.inner.lcd_info;
        let data = raw_data
            .into_par_iter()
            .map_with(
                lcd_info,
                |lcd_info, (idx, raw_data, data_file_path, shared_data_index)| {
                    let lcd_info = &lcd_info[shared_data_index];
                    let ch_data = lcd_info.convert_data(&raw_data).map_err(|_err| {
                        super::QueryError::InvalidData {
                            path: data_file_path.clone(),
                        }
                    })?;

                    Ok((idx, ch_data))
                },
            )
            .collect::<Result<Vec<_>, _>>()?;

        let (idx, data) = data.into_iter().unzip();
        let data = super::Data::new(idx, data).unwrap();
        Ok(data)
    }

    fn query_metadata(
        &mut self,
        query: &super::MetadataQuery,
    ) -> Result<super::Metadata, super::QueryError> {
        match query {
            super::MetadataQuery::All => self.metadata_all(),
            super::MetadataQuery::Dataset => self.inner.metadata_dataset(),
            super::MetadataQuery::SharedData => self.inner.metadata_shared(),
            super::MetadataQuery::Index(index_query) => match index_query {
                super::IndexQuery::All => todo!("FileReader::query_metadata(IndexQuery::All)"),
                super::IndexQuery::Index(index) => {
                    todo!("FileReader::query_metadata(IndexQuery::Index)")
                }
                super::IndexQuery::PixelRect(pixel_rect) => {
                    todo!("FileReader::query_metadata(IndexQuery::PixelRect)")
                }
                super::IndexQuery::Pixel(pixel) => self.inner.metadata_index_pixel(pixel),
            },
            super::MetadataQuery::Segment { index, segment } => {
                todo!("FileReader::query_metadata(SegmentQuery)")
            }
        }
    }
}

impl FileReader {
    fn metadata_all(&mut self) -> Result<super::Metadata, super::QueryError> {
        let properties = (0..self.inner.archive.len())
            .into_par_iter()
            .map_init(
                {
                    let metadata = self.inner.archive.metadata();
                    let file_path = &self.file_path;
                    move || {
                        let file = fs::File::open(file_path).expect("could not open file");
                        unsafe { zip::ZipArchive::unsafe_new_with_metadata(file, metadata.clone()) }
                    }
                },
                |archive, idx| {
                    let mut file = archive
                        .by_index(idx)
                        .map_err(|err| super::QueryError::Zip(err))?;

                    metadata_index_from_file_path(
                        file.name(),
                        &self.inner.dataset_info.position_pattern,
                    )
                    .map_err(|err| super::QueryError::ZipFile {
                        path: PathBuf::from(file.name()),
                        error: err,
                    })
                    .map(|maybe_index| {
                        maybe_index
                            .map(|index| {
                                super::Properties::new(&mut file)
                                    .map(|property| (index, property))
                                    .map_err(|_| super::QueryError::InvalidFormat {
                                        path: PathBuf::from(file.name()),
                                        cause: "file could not be read as properties".to_string(),
                                    })
                            })
                            .transpose()
                    })
                    .flatten()
                },
            )
            .collect::<Result<Vec<_>, _>>()?;

        let (indices, data) = properties
            .into_iter()
            .filter_map(|index| index)
            .unzip::<_, _, Vec<_>, Vec<_>>();

        Ok(super::Metadata::from_parts(indices, data).expect("indices and data are compatible"))
    }
}

impl crate::ArchiveReader for FileReader {
    fn files(&self) -> Vec<&str> {
        self.inner.files()
    }

    fn len(&self) -> usize {
        self.inner.len()
    }
}

pub struct Reader<R> {
    archive: zip::ZipArchive<R>,
    dataset_info: DatasetInfo,
    lcd_info: Vec<lcd_info::LcdInfo>,
}

impl<R> Reader<R>
where
    R: io::Read + io::Seek,
{
    pub fn new(mut archive: zip::ZipArchive<R>) -> Result<Self, super::Error> {
        let dataset_info = {
            let path = PathBuf::from(utils::DATASET_PROPERTIES_FILE);
            let mut properties = archive.by_path(&path).map_err(|error| super::Error::Zip {
                path: path.clone(),
                error,
            })?;

            let properties =
                Properties::new(&mut properties).map_err(|_err| super::Error::InvalidFormat {
                    path: path.clone(),
                    cause: "invalid properties file".to_string(),
                })?;

            Self::_init_dataset_info(&properties)?
        };

        let shared_data = {
            let path = utils::shared_data_properties_path();
            let mut properties = archive.by_path(&path).map_err(|error| super::Error::Zip {
                path: path.clone(),
                error,
            })?;

            let properties =
                Properties::new(&mut properties).map_err(|_err| super::Error::InvalidFormat {
                    path: path.clone(),
                    cause: "invalid properties file".to_string(),
                })?;
            SharedData { inner: properties }
        };
        let lcd_info = Self::_init_lcd_infos(&shared_data)?;

        Ok(Self {
            archive,
            dataset_info,
            lcd_info,
        })
    }

    fn _init_dataset_info(properties: &Properties) -> Result<DatasetInfo, super::Error> {
        let Some(index_type) = properties.get(DatasetProperties::INDEX_TYPE_KEY) else {
            return Err(super::Error::InvalidFormat {
                path: PathBuf::from(utils::DATASET_PROPERTIES_FILE),
                cause: format!(
                    "expected key `{}` to exist",
                    DatasetProperties::INDEX_TYPE_KEY
                ),
            });
        };
        let index = match index_type.as_str() {
            "range" => {
                let Some(min) = properties.get(DatasetProperties::INDEX_MIN_KEY) else {
                    return Err(super::Error::InvalidFormat {
                        path: PathBuf::from(utils::DATASET_PROPERTIES_FILE),
                        cause: format!(
                            "expected key `{}` to exist",
                            DatasetProperties::INDEX_MIN_KEY
                        ),
                    });
                };
                let Ok(min) = min.parse::<IndexType>() else {
                    return Err(super::Error::InvalidFormat {
                        path: PathBuf::from(utils::DATASET_PROPERTIES_FILE),
                        cause: format!("invalid value of `{}`", DatasetProperties::INDEX_MIN_KEY),
                    });
                };

                let Some(max) = properties.get(DatasetProperties::INDEX_MAX_KEY) else {
                    return Err(super::Error::InvalidFormat {
                        path: PathBuf::from(utils::DATASET_PROPERTIES_FILE),
                        cause: format!(
                            "expected key `{}` to exist",
                            DatasetProperties::INDEX_MAX_KEY
                        ),
                    });
                };
                let Ok(max) = max.parse::<IndexType>() else {
                    return Err(super::Error::InvalidFormat {
                        path: PathBuf::from(utils::DATASET_PROPERTIES_FILE),
                        cause: format!("invalid value of `{}`", DatasetProperties::INDEX_MAX_KEY),
                    });
                };

                Index::Range { min, max }
            }

            _ => {
                return Err(super::Error::InvalidFormat {
                    path: PathBuf::from(utils::DATASET_PROPERTIES_FILE),
                    cause: format!("invalid value of `{}`", DatasetProperties::INDEX_TYPE_KEY),
                });
            }
        };

        let position_pattern =
            PositionPattern::from_properties(properties).map_err(|err| match err {
                PropertyError::NotFound(key) => super::Error::InvalidFormat {
                    path: PathBuf::from(utils::DATASET_PROPERTIES_FILE),
                    cause: format!("`property {key}` not found"),
                },
                PropertyError::InvalidValue(key) => super::Error::InvalidFormat {
                    path: PathBuf::from(utils::DATASET_PROPERTIES_FILE),
                    cause: format!("invalid value of `{key}`"),
                },
            })?;

        Ok(DatasetInfo {
            index,
            position_pattern,
        })
    }

    fn _init_lcd_infos(properties: &SharedData) -> Result<Vec<lcd_info::LcdInfo>, super::Error> {
        let infos_count =
            properties::extract_value!(properties, SharedData::LCD_INFOS_COUNT_KEY, parse usize)
                .map_err(|err| match err {
                    PropertyError::NotFound(key) => super::Error::InvalidFormat {
                        path: utils::shared_data_properties_path(),
                        cause: format!(" property `{key}` not found",),
                    },
                    PropertyError::InvalidValue(key) => super::Error::InvalidFormat {
                        path: utils::shared_data_properties_path(),
                        cause: format!("invalid value for property `{key}`",),
                    },
                })?;

        (0..infos_count)
            .map(|idx| Self::_init_lcd_info(properties, idx))
            .collect()
    }

    fn _init_lcd_info(
        properties: &SharedData,
        index: usize,
    ) -> Result<lcd_info::LcdInfo, super::Error> {
        lcd_info::LcdInfo::from_properties(properties, index).map_err(|err| match err {
            PropertyError::NotFound(key) => super::Error::InvalidFormat {
                path: utils::shared_data_properties_path(),
                cause: format!("property `{key}` not found"),
            },
            PropertyError::InvalidValue(key) => super::Error::InvalidFormat {
                path: utils::shared_data_properties_path(),
                cause: format!("invalid property value of `{key}`"),
            },
        })
    }
}

impl<R> Reader<R>
where
    R: io::Read + io::Seek,
{
    pub fn get_data_index_segment_channel(
        &mut self,
        index: IndexType,
        segment: SegmentType,
        channel: impl fmt::Display,
    ) -> Result<Vec<Value>, DataError> {
        let segment_properties_path = utils::index_segment_properties_path(index, segment);
        let segment_properties = {
            let mut segment_properties =
                self.archive
                    .by_path(&segment_properties_path)
                    .map_err(|error| DataError::Zip {
                        path: segment_properties_path.clone(),
                        error,
                    })?;
            let segment_properties =
                Properties::new(&mut segment_properties).map_err(|_| DataError::InvalidFormat {
                    path: segment_properties_path.clone(),
                    cause: "file could not be read as properties".to_string(),
                })?;
            SegmentProperties {
                inner: segment_properties,
            }
        };

        let channel_data = channel_data::ChannelData::from(&segment_properties, &channel).map_err(
            |err| match err {
                PropertyError::NotFound(key) => DataError::InvalidFormat {
                    path: segment_properties_path.clone(),
                    cause: format!("property `{key}` not found"),
                },
                PropertyError::InvalidValue(key) => DataError::InvalidFormat {
                    path: segment_properties_path.clone(),
                    cause: format!("invalid value of `{key}`"),
                },
            },
        )?;
        let lcd_info = &self.lcd_info[channel_data.shared_data_index()];

        let data_file_path = {
            let path = utils::index_segment_path(index, segment);
            let path = format!(
                "{}/{}",
                path.to_string_lossy(),
                channel_data.file_path().to_string_lossy()
            );
            PathBuf::from(path)
        };

        let mut data_file =
            self.archive
                .by_path(&data_file_path)
                .map_err(|error| DataError::Zip {
                    path: data_file_path.clone(),
                    error,
                })?;
        let mut data = Vec::with_capacity(data_file.size() as usize);
        data_file
            .read_to_end(&mut data)
            .map_err(|err| DataError::Zip {
                path: data_file_path.clone(),
                error: zip::result::ZipError::Io(err),
            })?;

        let data = lcd_info
            .convert_data(&data)
            .map_err(|_err| DataError::InvalidData {
                path: data_file_path.clone(),
            })?;

        Ok(data)
    }
}

impl<R> super::QIMapReader for Reader<R>
where
    R: io::Read + io::Seek,
{
    fn query_data(&mut self, query: &super::DataQuery) -> Result<super::Data, super::QueryError> {
        let indices = self._data_query_indices(query)?;
        let mut data_idx = Vec::with_capacity(indices.len());
        for index in indices {
            let index_data = utils::index_data(&mut self.archive, index)?;
            let segments = match &query.segment {
                super::SegmentQuery::All => (0..index_data.segment_count()).collect::<Vec<_>>(),
                super::SegmentQuery::Indices(indices) => indices.clone(),
            };

            for segment in segments {
                let segment_properties =
                    utils::segment_properties(&mut self.archive, index, segment)?;
                let segment_data = utils::segment_data(&segment_properties, index)?;
                let channels = match &query.channel {
                    super::ChannelQuery::All => segment_data.channels().clone(),
                    super::ChannelQuery::Include(channels) => {
                        let mut channels = channels.clone();
                        channels.retain(|channel| segment_data.channels().contains(channel));
                        channels
                    }
                };

                for channel in channels {
                    let channel_data =
                        utils::channel_data(&segment_properties, &channel, index, segment)?;

                    data_idx.push((
                        super::DataIndex {
                            index,
                            segment,
                            channel,
                        },
                        index,
                        channel_data.shared_data_index(),
                        channel_data.file_path().clone(),
                    ))
                }
            }
        }

        let mut data = Vec::with_capacity(data_idx.len());
        for (idx, index, shared_data_index, file_path) in data_idx {
            let lcd_info = &self.lcd_info[shared_data_index];
            let data_file_path = {
                let path = utils::index_segment_path(index, idx.segment);
                let path = format!("{}/{}", path.to_string_lossy(), file_path.to_string_lossy());
                PathBuf::from(path)
            };

            let mut data_file = self.archive.by_path(&data_file_path).map_err(|error| {
                super::QueryError::ZipFile {
                    path: data_file_path.clone(),
                    error,
                }
            })?;
            let mut raw_data = Vec::with_capacity(data_file.size() as usize);
            data_file
                .read_to_end(&mut raw_data)
                .map_err(|err| super::QueryError::ZipFile {
                    path: data_file_path.clone(),
                    error: zip::result::ZipError::Io(err),
                })?;

            let ch_data = lcd_info.convert_data(&raw_data).map_err(|_err| {
                super::QueryError::InvalidData {
                    path: data_file_path.clone(),
                }
            })?;

            data.push((idx, ch_data));
        }

        let (idx, data) = data.into_iter().unzip();
        let data = super::Data::new(idx, data).unwrap();
        Ok(data)
    }

    fn query_metadata(
        &mut self,
        query: &super::MetadataQuery,
    ) -> Result<super::Metadata, super::QueryError> {
        match query {
            super::MetadataQuery::All => self.metadata_all(),
            super::MetadataQuery::Dataset => self.metadata_dataset(),
            super::MetadataQuery::SharedData => self.metadata_shared(),
            super::MetadataQuery::Index(query) => match query {
                super::IndexQuery::All => todo!("Reader::query_metadata(IndexQuery::All)"),
                super::IndexQuery::Index(index) => {
                    todo!("Reader::query_metadata(IndexQuery::Index)")
                }
                super::IndexQuery::PixelRect(rect) => {
                    todo!("Reader::query_metadata(IndexQuery::PixelRect)")
                }
                super::IndexQuery::Pixel(pixel) => self.metadata_index_pixel(pixel),
            },
            super::MetadataQuery::Segment { index, segment } => {
                todo!("Reader::query_metadata(SegmentQuery)")
            }
        }
    }
}

impl<R> crate::ArchiveReader for Reader<R>
where
    R: io::Read + io::Seek,
{
    fn files(&self) -> Vec<&str> {
        self.archive.file_names().collect()
    }

    fn len(&self) -> usize {
        self.archive.len()
    }
}

impl<R> Reader<R>
where
    R: io::Read + io::Seek,
{
    fn _data_query_indices(
        &mut self,
        query: &super::DataQuery,
    ) -> Result<Vec<IndexType>, super::QueryError> {
        match &query.index {
            super::IndexQuery::All => match self.dataset_info.index {
                Index::Range { min, max } => Ok((min..=max).collect::<Vec<_>>()),
            },

            super::IndexQuery::Index(index) => Ok(vec![*index]),

            super::IndexQuery::PixelRect(rect) => rect
                .iter()
                .map(|pixel| {
                    self.dataset_info
                        .position_pattern
                        .pixel_to_index(&pixel)
                        .ok_or(super::QueryError::OutOfBounds(pixel))
                })
                .collect::<Result<Vec<_>, _>>(),

            super::IndexQuery::Pixel(pixel) => {
                let idx = self
                    .dataset_info
                    .position_pattern
                    .pixel_to_index(pixel)
                    .ok_or(super::QueryError::OutOfBounds(pixel.clone()))?;
                Ok(vec![idx])
            }
        }
    }
}

impl<R> Reader<R>
where
    R: io::Read + io::Seek,
{
    fn metadata_all(&mut self) -> Result<super::Metadata, super::QueryError> {
        let mut metadata = super::Metadata::with_capacity(self.archive.len() / 2);
        for idx in 0..self.archive.len() {
            let mut file = self
                .archive
                .by_index(idx)
                .map_err(|err| super::QueryError::Zip(err))?;

            let index =
                metadata_index_from_file_path(file.name(), &self.dataset_info.position_pattern)
                    .map_err(|err| super::QueryError::ZipFile {
                        path: PathBuf::from(file.name()),
                        error: err,
                    })?;
            let Some(index) = index else {
                continue;
            };

            let properties = super::Properties::new(&mut file).map_err(|_| {
                super::QueryError::InvalidFormat {
                    path: PathBuf::from(file.name()),
                    cause: "file could not be read as properties".to_string(),
                }
            })?;

            metadata.insert(index, properties);
        }

        Ok(metadata)
    }

    fn metadata_dataset(&mut self) -> Result<super::Metadata, super::QueryError> {
        let mut properties = self
            .archive
            .by_path(utils::DATASET_PROPERTIES_FILE)
            .map_err(|error| super::QueryError::ZipFile {
                path: PathBuf::from(utils::DATASET_PROPERTIES_FILE),
                error,
            })?;

        let properties =
            Properties::new(&mut properties).map_err(|_| super::QueryError::InvalidFormat {
                path: PathBuf::from(utils::DATASET_PROPERTIES_FILE),
                cause: "file could not be read as properties".to_string(),
            })?;

        let indices = vec![super::MetadataIndex::Dataset];
        let data = vec![properties];
        Ok(super::Metadata::from_parts(indices, data).unwrap())
    }

    fn metadata_shared(&mut self) -> Result<super::Metadata, super::QueryError> {
        let data_path = utils::shared_data_properties_path();
        let mut properties =
            self.archive
                .by_path(&data_path)
                .map_err(|error| super::QueryError::ZipFile {
                    path: data_path.clone(),
                    error,
                })?;

        let properties =
            Properties::new(&mut properties).map_err(|_| super::QueryError::InvalidFormat {
                path: data_path.clone(),
                cause: "file could not be read as properties".to_string(),
            })?;

        let indices = vec![super::MetadataIndex::SharedData];
        let data = vec![properties];
        Ok(super::Metadata::from_parts(indices, data).unwrap())
    }

    fn metadata_index_pixel(
        &mut self,
        pixel: &super::Pixel,
    ) -> Result<super::Metadata, super::QueryError> {
        let Some(index) = self.dataset_info.position_pattern.pixel_to_index(pixel) else {
            return Err(super::QueryError::OutOfBounds(pixel.clone()));
        };
        let data_path = utils::index_properties_path(index);
        let mut properties =
            self.archive
                .by_path(&data_path)
                .map_err(|error| super::QueryError::ZipFile {
                    path: data_path.clone(),
                    error,
                })?;
        let properties =
            Properties::new(&mut properties).map_err(|_| super::QueryError::InvalidFormat {
                path: data_path.clone(),
                cause: "file could not be read as properties".to_string(),
            })?;

        let idx = vec![super::MetadataIndex::Index(index)];
        let data = vec![properties];
        Ok(super::Metadata::from_parts(idx, data).unwrap())
    }
}

fn metadata_index_from_file_path(
    filename: &str,
    position_pattern: &PositionPattern,
) -> Result<Option<super::MetadataIndex>, zip::result::ZipError> {
    const INDEX_PREFIX: &str = "index/";
    const SEGMENT_PREFIX: &str = "segments/";

    if filename == super::PROPERTIES_FILE_PATH {
        return Ok(Some(super::MetadataIndex::Dataset));
    } else if filename == format!("{}/{}", SHARED_DATA_DIR, super::PROPERTIES_FILE_PATH) {
        return Ok(Some(super::MetadataIndex::SharedData));
    } else if filename.ends_with(super::SEGMENT_PROPERTIES_FILE_PATH) {
        let Some((index_str, _)) = filename[INDEX_PREFIX.len()..].split_once("/") else {
            return Err(zip::result::ZipError::InvalidArchive(
                std::borrow::Cow::Borrowed("invalid file path"),
            ));
        };

        let Ok(index) = index_str.parse::<IndexType>() else {
            return Err(zip::result::ZipError::InvalidArchive(
                std::borrow::Cow::Borrowed("invalid file path"),
            ));
        };

        let Some((_, segment_str)) = filename
            [..filename.len() - super::SEGMENT_PROPERTIES_FILE_PATH.len() - 1]
            .rsplit_once("/")
        else {
            return Err(zip::result::ZipError::InvalidArchive(
                std::borrow::Cow::Borrowed("invalid file path"),
            ));
        };

        let Ok(segment) = segment_str.parse::<SegmentType>() else {
            return Err(zip::result::ZipError::InvalidArchive(
                std::borrow::Cow::Borrowed("invalid file path"),
            ));
        };

        return Ok(Some(super::MetadataIndex::Segment { index, segment }));
    } else if filename.starts_with(INDEX_PREFIX)
        && filename.ends_with(&format!("/{}", super::PROPERTIES_FILE_PATH))
    {
        let Some((index_str, _)) = filename[INDEX_PREFIX.len()..].split_once("/") else {
            return Err(zip::result::ZipError::InvalidArchive(
                std::borrow::Cow::Borrowed("invalid file path"),
            ));
        };

        let Ok(index) = index_str.parse::<IndexType>() else {
            return Err(zip::result::ZipError::InvalidArchive(
                std::borrow::Cow::Borrowed("invalid file path"),
            ));
        };

        return Ok(Some(super::MetadataIndex::Index(index)));
    } else {
        return Ok(None);
    }
}

struct DatasetProperties;
impl DatasetProperties {
    const INDEX_TYPE_KEY: &str = "quantitative-imaging-map.indexes.type";
    const INDEX_MIN_KEY: &str = "quantitative-imaging-map.indexes.min";
    const INDEX_MAX_KEY: &str = "quantitative-imaging-map.indexes.max";
}

pub struct DatasetInfo {
    index: Index,
    position_pattern: PositionPattern,
}

enum Index {
    Range { min: IndexType, max: IndexType },
}

struct PositionPattern {
    numbering: Numbering,
    kind: PositionPatternType,
}

impl PositionPattern {
    const TYPE_KEY: &str = "quantitative-imaging-map.position-pattern.type";
    const NUMBERING_KEY: &str = "quantitative-imaging-map.position-pattern.numbering";
}

impl PositionPattern {
    pub fn from_properties(properties: &Properties) -> Result<Self, PropertyError> {
        let numbering =
            properties::extract_value!(properties, Self::NUMBERING_KEY, from_str Numbering)?;

        let kind = PositionPatternType::from_properties(properties)?;

        Ok(Self { numbering, kind })
    }
}

impl PositionPattern {
    /// # Returns
    /// `None` if pixel coordinate is invalid.
    pub fn pixel_to_index(&self, pixel: &super::Pixel) -> Option<IndexType> {
        match &self.kind {
            PositionPatternType::Grid(grid) => {
                if pixel.i >= grid.i_length as IndexType || pixel.j >= grid.j_length as IndexType {
                    return None;
                }

                Some(pixel.j * grid.i_length as IndexType + pixel.i)
            }
        }
    }

    /// # Returns
    /// `None` if index is invalid.
    pub fn index_to_pixel(&self, index: IndexType) -> Option<super::Pixel> {
        match &self.kind {
            PositionPatternType::Grid(grid) => {
                let y = index / grid.i_length as IndexType;
                if y >= grid.j_length as IndexType {
                    return None;
                }
                let x = index % grid.i_length as IndexType;
                return Some(super::Pixel { i: x, j: y });
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
    pub fn from_properties(properties: &Properties) -> Result<Self, PropertyError> {
        let kind = properties::extract_value!(properties, PositionPattern::TYPE_KEY)?;
        match kind.as_str() {
            "grid-position-pattern" => {
                let grid = Grid::from_properties(properties)?;
                Ok(Self::Grid(grid))
            }
            _ => Err(PropertyError::InvalidValue(
                PositionPattern::TYPE_KEY.to_string(),
            )),
        }
    }
}

struct Grid {
    x_center: f64,
    y_center: f64,
    u_length: f64,
    v_length: f64,
    unit: String,
    i_length: u16,
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
}

impl Grid {
    pub fn from_properties(properties: &Properties) -> Result<Self, PropertyError> {
        let x_center = properties::extract_value!(properties, Self::X_CENTER_KEY, parse f64)?;
        let y_center = properties::extract_value!(properties, Self::Y_CENTER_KEY, parse f64)?;
        let u_length = properties::extract_value!(properties, Self::U_LENGTH_KEY, parse f64)?;
        let v_length = properties::extract_value!(properties, Self::V_LENGTH_KEY, parse f64)?;
        let unit = properties::extract_value!(properties, Self::UNIT_KEY)?;
        let i_length = properties::extract_value!(properties, Self::I_LENGTH_KEY, parse u16)?;
        let j_length = properties::extract_value!(properties, Self::J_LENGTH_KEY, parse u16)?;

        Ok(Self {
            x_center,
            y_center,
            u_length,
            v_length,
            unit: unit.clone(),
            i_length,
            j_length,
        })
    }
}

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

mod utils {
    use super::{super::PROPERTIES_FILE_PATH, IndexType, SegmentType};
    use std::{fmt, io, path::PathBuf};
    use zip::ZipArchive;

    pub const INDEX_DIR: &str = "index";
    pub const SEGMENT_DIR: &str = "segments";
    pub const SHARED_DATA_DIR: &str = "shared-data";
    pub const DATASET_PROPERTIES_FILE: &str = "header.properties";
    pub const INDEX_PROPERTIES_FILE: &str = "header.properties";
    pub const SEGMENT_PROPERTIES_FILE: &str = "segment-header.properties";
    pub const SHARED_DATA_PROPERTIES_FILE: &str = "header.properties";
    pub const PROPERTIES_KEY_SEGMENT_CHANNELS_LIST: &str = "channels.list";

    pub fn properties_path() -> PathBuf {
        PathBuf::from(PROPERTIES_FILE_PATH)
    }

    pub fn index_properties_path(index: IndexType) -> PathBuf {
        let path = format!("{INDEX_DIR}/{index}/{INDEX_PROPERTIES_FILE}");
        PathBuf::from(path)
    }

    pub fn index_segment_path(index: IndexType, segment: SegmentType) -> PathBuf {
        let path = format!("{INDEX_DIR}/{index}/{SEGMENT_DIR}/{segment}");
        PathBuf::from(path)
    }

    pub fn index_segment_properties_path(index: IndexType, segment: SegmentType) -> PathBuf {
        let path = format!("{INDEX_DIR}/{index}/{SEGMENT_DIR}/{segment}/{SEGMENT_PROPERTIES_FILE}");
        PathBuf::from(path)
    }

    pub fn shared_data_properties_path() -> PathBuf {
        let path = format!("{SHARED_DATA_DIR}/{SHARED_DATA_PROPERTIES_FILE}");
        PathBuf::from(path)
    }

    pub fn index_data<R>(
        archive: &mut ZipArchive<R>,
        index: IndexType,
    ) -> Result<super::index_data::IndexData, super::super::QueryError>
    where
        R: io::Read + io::Seek,
    {
        use super::{super::QueryError, PropertyError, index_data, properties};

        let index_properties_path = index_properties_path(index);
        let mut file =
            archive
                .by_path(&index_properties_path)
                .map_err(|error| QueryError::ZipFile {
                    path: index_properties_path.clone(),
                    error,
                })?;
        let properties =
            properties::Properties::new(&mut file).map_err(|_err| QueryError::InvalidFormat {
                path: index_properties_path.clone(),
                cause: "invalid property file".to_string(),
            })?;

        index_data::IndexData::from_properties(&properties).map_err(|err| match err {
            PropertyError::NotFound(key) => QueryError::InvalidFormat {
                path: index_properties_path.clone(),
                cause: format!("property `{key}` not found"),
            },
            PropertyError::InvalidValue(key) => QueryError::InvalidFormat {
                path: index_properties_path.clone(),
                cause: format!("invalid value for `{key}`"),
            },
        })
    }

    pub fn segment_properties<R>(
        archive: &mut zip::ZipArchive<R>,
        index: IndexType,
        segment: SegmentType,
    ) -> Result<super::SegmentProperties, super::super::QueryError>
    where
        R: io::Read + io::Seek,
    {
        use super::{super::QueryError, properties};

        let segment_properties_path = index_segment_properties_path(index, segment);
        let mut properties =
            archive
                .by_path(&segment_properties_path)
                .map_err(|error| QueryError::ZipFile {
                    path: segment_properties_path.clone(),
                    error,
                })?;
        let properties = properties::Properties::new(&mut properties).map_err(|_| {
            QueryError::InvalidFormat {
                path: segment_properties_path.clone(),
                cause: "file could not be read as properties".to_string(),
            }
        })?;

        Ok(super::SegmentProperties { inner: properties })
    }

    /// # Notes
    /// + `index` only used for error reporting.
    pub fn segment_data(
        properties: &super::SegmentProperties,
        index: IndexType,
    ) -> Result<super::segment_data::SegmentData, super::super::QueryError> {
        use super::{super::QueryError, PropertyError, segment_data};

        segment_data::SegmentData::from(properties).map_err(|err| match err {
            PropertyError::NotFound(key) => QueryError::InvalidFormat {
                path: index_properties_path(index),
                cause: format!("property `{key}` not found"),
            },
            PropertyError::InvalidValue(key) => QueryError::InvalidFormat {
                path: index_properties_path(index),
                cause: format!("invalid value for `{key}`"),
            },
        })
    }

    /// # Notes
    /// + `index` and `segment` only used for error reporting.
    pub fn channel_data(
        segment_properties: &super::SegmentProperties,
        channel: impl fmt::Display,
        index: IndexType,
        segment: SegmentType,
    ) -> Result<super::channel_data::ChannelData, super::super::QueryError> {
        use super::{super::QueryError, PropertyError, channel_data};

        channel_data::ChannelData::from(segment_properties, &channel).map_err(|err| match err {
            PropertyError::NotFound(key) => QueryError::InvalidFormat {
                path: index_segment_properties_path(index, segment),
                cause: format!("property `{key}` not found"),
            },
            PropertyError::InvalidValue(key) => QueryError::InvalidFormat {
                path: index_segment_properties_path(index, segment),
                cause: format!("invalid value of `{key}`"),
            },
        })
    }
}

mod index_data {
    use super::SegmentType;
    use crate::properties::{self, Properties, PropertyError};

    pub struct IndexData {
        segment_count: SegmentType,
    }

    impl IndexData {
        const SEGMENT_COUNT_KEY: &str = "quantitative-imaging-series.force-segments.count";
    }

    impl IndexData {
        pub fn from_properties(properties: &Properties) -> Result<Self, PropertyError> {
            let segment_count =
                properties::extract_value!(properties, Self::SEGMENT_COUNT_KEY, parse SegmentType)?;

            Ok(Self { segment_count })
        }
    }

    impl IndexData {
        pub fn segment_count(&self) -> SegmentType {
            self.segment_count
        }
    }
}

mod segment_data {
    use super::SegmentProperties;
    use crate::properties::{self, PropertyError};

    pub struct SegmentData {
        channels: Vec<String>,
    }

    impl SegmentData {
        const CHANNEL_LIST_KEY: &str = "channels.list";
    }

    impl SegmentData {
        pub fn from(properties: &SegmentProperties) -> Result<Self, PropertyError> {
            let channels = properties::extract_value!(properties, Self::CHANNEL_LIST_KEY)?;
            let channels = channels
                .split_ascii_whitespace()
                .map(|channel| channel.to_string())
                .collect();

            Ok(Self { channels })
        }

        pub fn channels(&self) -> &Vec<String> {
            &self.channels
        }
    }
}

mod channel_data {
    use super::DataFileFormat;
    use crate::{
        properties::{self, PropertyError},
        qi_map::v2_0::SegmentProperties,
    };
    use std::{fmt, path::PathBuf};

    #[derive(Debug)]
    pub struct ChannelData {
        file_path: PathBuf,
        file_format: DataFileFormat,
        num_points: usize,
        shared_data_index: usize,
    }

    impl ChannelData {
        #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
        pub fn from(
            properties: &SegmentProperties,
            channel: impl fmt::Display,
        ) -> Result<Self, PropertyError> {
            let file_path = properties::extract_value!(
                properties,
                SegmentProperties::channel_data_file_name_key(&channel)
            )?;
            let file_format = properties::extract_value!(properties,SegmentProperties::channel_data_file_format_key(&channel), from_str DataFileFormat )?;
            let num_points = properties::extract_value!(properties, SegmentProperties::channel_data_num_points_key(&channel), parse usize)?;
            let shared_data_index = properties::extract_value!(properties, SegmentProperties::channel_shared_data_index_key(&channel), parse usize)?;

            let data = Self {
                file_path: PathBuf::from(file_path),
                file_format,
                num_points,
                shared_data_index,
            };
            #[cfg(feature = "tracing")]
            tracing::trace!(?data);

            Ok(data)
        }
    }

    impl ChannelData {
        pub fn file_path(&self) -> &PathBuf {
            &self.file_path
        }

        pub fn file_format(&self) -> DataFileFormat {
            self.file_format
        }
        pub fn num_points(&self) -> usize {
            self.num_points
        }
        pub fn shared_data_index(&self) -> usize {
            self.shared_data_index
        }
    }
}
