//! Voltage spectroscopy data (`.jpk-voltage-ramp`) reader.

use std::path::Path;

pub const VOLTAGE_SPECTROSCOPY_FILE_EXT: &str = "jpk-voltage-ramp";

/// Check if the given path matches the epxected naming for a voltage spectroscopy dataset file.
pub fn validate_path(path: impl AsRef<Path>) -> bool {
    let path = path.as_ref();
    if !path.is_file() {
        return false;
    }
    let Some(ext) = path.extension() else {
        return false;
    };
    let Some(ext) = ext.to_str() else {
        return false;
    };

    ext == VOLTAGE_SPECTROSCOPY_FILE_EXT
}

/// Voltage spectroscopy data (`.jpk-voltage-ramp`) file format version 2.0 reader.
pub mod v2_0 {
    use crate::dataset::{
        properties::{self, extract_value},
        v2_0 as dataset,
        v2_0::DatasetReader,
    };
    use polars::{
        prelude::{self as pl, ChunkFull, IntoColumn},
        series::IntoSeries,
    };
    use rayon::prelude::*;
    use std::{fs, io, iter, path::PathBuf};

    const DATASET_DATA_FILE_PROPERTY_VALUE: &str = "spm-forcefile";
    const DATASET_TYPE_PROPERTY_VALUE: &str = "voltage-spectroscopy-segment-series";
    const SEGMENT_COUNTS_PROPERTY_KEY: &str =
        "voltage-spectroscopy-segment-series.force-segments.count";
    const SEGMENT_NUM_POINTS_PROPERTY_KEY: &str = "force-segment-header.num-points";
    const POSITION_X_PROPERTY_KEY: &str = "voltage-spectroscopy-segment-series.header.position.x";
    const POSITION_Y_PROPERTY_KEY: &str = "voltage-spectroscopy-segment-series.header.position.y";

    #[derive(derive_more::Deref)]
    pub struct Reader<R> {
        inner: DatasetReader<R>,
    }

    impl<R> Reader<R>
    where
        R: io::Read + io::Seek,
    {
        pub fn new(
            archive: zip::ZipArchive<R>,
        ) -> Result<Self, crate::dataset::error::Error<crate::dataset::error::Dataset>> {
            let reader = DatasetReader::new(archive)?;
            Ok(Self { inner: reader })
        }

        /// # Returns
        /// `Err((properties path, error))`
        pub fn segment_properties(
            &mut self,
            segment: dataset::SegmentType,
        ) -> Result<
            dataset::properties::segment::Properties,
            crate::dataset::error::Error<dataset::error::Properties>,
        > {
            let segment_path = dataset::utils::segment_path(segment);
            self.inner.segment_properties(segment_path)
        }

        /// Get data for a segment's channel.
        ///
        /// # Returns
        /// `Err((properties path, error))`
        ///
        /// # Notes
        /// + Loads the segment's properties file.
        pub fn segment_channel_data(
            &mut self,
            segment: dataset::SegmentType,
            channel: impl AsRef<str>,
        ) -> Result<pl::Series, crate::dataset::error::Error<dataset::error::SegmentChannelData>>
        {
            let properties = self
                .segment_properties(segment)
                .map_err(|err| err.map_err(|err| err.into()))?;
            let channel_info = properties
                .channel_info(channel.as_ref())
                .map_err(|err| err.map_err(Into::into))?;

            let segment_path = dataset::utils::segment_path(segment);
            let data = self
                .inner
                .channel_data(&segment_path, &channel_info)
                .map_err(|err| err.map_err(Into::into))?;
            Ok(pl::Float64Chunked::from_vec(channel.as_ref().into(), data).into_series())
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

        /// Number of segments in the dataset as indicated in the dataset properties.
        pub fn segments_count(&self) -> Result<dataset::SegmentType, properties::error::Property> {
            extract_value!(self.inner.dataset_properties(), SEGMENT_COUNTS_PROPERTY_KEY, parse dataset::SegmentType)
        }

        /// `(x, y)` coordinate.
        pub fn position(&self) -> Result<(f64, f64), properties::error::Property> {
            let properties = self.inner.dataset_properties();
            let x = extract_value!(properties, POSITION_X_PROPERTY_KEY, parse f64)?;
            let y = extract_value!(properties, POSITION_Y_PROPERTY_KEY, parse f64)?;
            Ok((x, y))
        }
    }

    /// Read a single voltage spectroscopy (`.jpk-voltage-ramp`) file.
    #[derive(derive_more::Deref)]
    pub struct FileReader {
        path: PathBuf,
        #[deref]
        inner: Reader<fs::File>,
    }

    impl FileReader {
        pub fn new(
            path: impl Into<PathBuf>,
        ) -> Result<Self, crate::dataset::error::Error<crate::dataset::error::Dataset>> {
            let path = path.into();
            let file = fs::File::open(&path).map_err(|err| crate::dataset::error::Error {
                paths: vec![path.clone()],
                error: crate::dataset::error::Dataset::OpenArchive(zip::result::ZipError::Io(err)),
            })?;
            let archive =
                zip::ZipArchive::new(file).map_err(|err| crate::dataset::error::Error {
                    paths: vec![path.clone()],
                    error: crate::dataset::error::Dataset::OpenArchive(err),
                })?;
            let inner = Reader::new(archive)?;
            Ok(Self { path, inner })
        }

        /// Create a new file reader with an archive.
        ///
        /// # Safety
        /// + It is left to the user to ensure that `path` and `archive` coincide.
        pub unsafe fn new_with_archive(
            path: impl Into<PathBuf>,
            archive: zip::ZipArchive<fs::File>,
        ) -> Result<Self, crate::dataset::error::Error<crate::dataset::error::Dataset>> {
            let inner = Reader::new(archive)?;
            Ok(Self {
                path: path.into(),
                inner,
            })
        }

        pub fn path(&self) -> &PathBuf {
            &self.path
        }

        /// Loads data from all segments and all channels.
        pub fn load_data_all(
            &mut self,
        ) -> Result<pl::DataFrame, crate::dataset::error::Error<error::DataFile>> {
            let segments_count =
                self.segments_count()
                    .map_err(|err| crate::dataset::error::Error {
                        paths: vec![PathBuf::from(crate::dataset::DATASET_PROPERTIES_FILE_PATH)],
                        error: err.into(),
                    })?;
            if segments_count == 0 {
                return Ok(pl::DataFrame::empty());
            }

            let mut seg_cols = Vec::with_capacity(segments_count as usize);
            let mut headers = Vec::new();
            for segment in 0..segments_count {
                let properties = self
                    .inner
                    .segment_properties(segment)
                    .map_err(|err| err.map_err(Into::into))?;
                let channels = properties
                    .channel_list()
                    .map_err(|err| err.map_err(Into::into))?;
                let mut scols = Vec::with_capacity(channels.len() + 1);
                for channel in channels {
                    let data = self
                        .inner
                        .segment_channel_data(segment, channel)
                        .map_err(|err| err.map_err(Into::into))?;
                    scols.push(data.into_column());
                    headers.push(channel.to_string());
                }

                let length =
                    extract_value!(properties, SEGMENT_NUM_POINTS_PROPERTY_KEY, parse usize)
                        .map_err(|err| crate::dataset::error::Error {
                            paths: vec![properties.path.clone()],
                            error: err.into(),
                        })?;
                let seg = pl::UInt8Chunked::full("segment".into(), segment, length).into_column();
                scols.push(seg);

                seg_cols.push(scols);
            }
            headers.sort();
            headers.dedup();
            let mut seg_col = Vec::with_capacity(segments_count as usize);
            for scols in seg_cols.iter_mut() {
                seg_col.push(scols.pop().expect("segment should have segment id column"));
            }

            let mut cols = Vec::with_capacity(headers.len());
            for header in headers.iter() {
                let mut data_cols = Vec::with_capacity(segments_count as usize);
                for (sidx, scols) in seg_cols.iter_mut().enumerate() {
                    let col_idx = scols.iter().position(|col| col.name() == header);
                    let col = match col_idx {
                        None => pl::Column::new_scalar(
                            header.into(),
                            pl::Scalar::null(pl::DataType::Float64),
                            seg_col[sidx].len(),
                        ),
                        Some(idx) => scols.swap_remove(idx),
                    };
                    assert_eq!(
                        col.len(),
                        seg_col[sidx].len(),
                        "data and segment index have different lengths"
                    );

                    data_cols.push(col);
                }
                cols.push(data_cols)
            }

            let seg_col = seg_col
                .into_iter()
                .reduce(|mut acc, elm| {
                    acc.append_owned(elm).unwrap();
                    acc
                })
                .expect("at least one segment should exist");

            let data_cols = cols
                .into_iter()
                .map(|dcols| {
                    dcols
                        .into_iter()
                        .reduce(|mut acc, elm| {
                            acc.append_owned(elm).unwrap();
                            acc
                        })
                        .expect("at least on data column should exist")
                })
                .collect::<Vec<_>>();

            let df = iter::once(seg_col).chain(data_cols).collect();
            let df = pl::DataFrame::new(df)
                .map_err(|err| crate::dataset::error::Error::new(err.into()))?;
            Ok(df)
        }
    }

    pub fn load_files(
        paths: &Vec<PathBuf>,
    ) -> Result<pl::DataFrame, crate::dataset::error::Error<error::DataCollection>> {
        let data = paths
            .into_par_iter()
            .map(|path| {
                let mut reader =
                    FileReader::new(path.clone()).map_err(|err| err.map_err(Into::into))?;
                let data = reader
                    .load_data_all()
                    .map_err(|err| err.map_err(Into::into))?;
                let xy = reader
                    .position()
                    .map_err(|err| crate::dataset::error::Error {
                        paths: vec![reader.path().clone()],
                        error: error::DataFile::Property(err).into(),
                    })?;

                Ok((xy, data))
            })
            .collect::<Result<Vec<_>, _>>()?;
        if data.len() == 0 {
            return Ok(pl::DataFrame::empty());
        }

        let (idx, df) = data.into_iter().unzip::<_, _, Vec<_>, Vec<_>>();
        let (xcols, ycols) = idx
            .into_iter()
            .enumerate()
            .map(|(idx, (x, y))| {
                let length = df[idx].height();
                let xcol = pl::Column::new_scalar(
                    "x".into(),
                    pl::Scalar::new(pl::DataType::Float64, x.into()),
                    length,
                );
                let ycol = pl::Column::new_scalar(
                    "y".into(),
                    pl::Scalar::new(pl::DataType::Float64, y.into()),
                    length,
                );
                (xcol, ycol)
            })
            .unzip::<_, _, Vec<_>, Vec<_>>();

        let xcol = xcols
            .into_iter()
            .reduce(|mut acc, elm| {
                acc.append_owned(elm).unwrap();
                acc
            })
            .expect("at least one x col should exist");

        let ycol = ycols
            .into_iter()
            .reduce(|mut acc, elm| {
                acc.append_owned(elm).unwrap();
                acc
            })
            .expect("at least one y col should exist");

        let mut df = df
            .into_iter()
            .reduce(|mut acc, elm| {
                acc.vstack_mut_owned(elm).unwrap();
                acc
            })
            .expect("at least one data frame should exist");

        df.with_column(xcol).unwrap();
        df.with_column(ycol).unwrap();

        Ok(df)
    }

    pub mod error {
        use std::io;

        use crate::dataset;

        /// An error occurred while reading the file of a dataset.
        #[derive(derive_more::From, Debug)]
        pub enum DataFile {
            /// Could not read a required property.
            Property(dataset::properties::error::Property),
            /// Could not read a required properties file.
            Properties(dataset::v2_0::error::Properties),
            /// Could not read of convert channel data.
            SegmentChannelData(dataset::v2_0::error::SegmentChannelData),
            /// Could not create a valid dataframe.
            Polars(polars::error::PolarsError),
        }

        #[derive(derive_more::From, Debug)]
        pub enum DataCollection {
            Io(io::Error),
            Dataset(dataset::error::Dataset),
            DataFile(DataFile),
        }
    }
}
