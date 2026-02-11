//! Voltage spectroscopy data reader.
//! (`.jpk-voltage-ramp`)

pub mod v2_0 {
    use crate::dataset::{
        DatasetError,
        properties::{self, extract_value},
        v2_0 as dataset,
        v2_0::DatasetReader,
    };
    use polars::prelude::{self as pl, ChunkFull, IntoColumn};
    use rayon::prelude::*;
    use std::{fs, io, iter, path::PathBuf};

    const VOLTAGE_SPECTROSCOPY_FILE_EXT: &str = "jpk-voltage-ramp";
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
        pub fn new(archive: ::zip::ZipArchive<R>) -> Result<Self, DatasetError> {
            let reader = DatasetReader::new(archive)?;
            Ok(Self { inner: reader })
        }

        pub fn segment_properties(
            &mut self,
            segment: dataset::SegmentType,
        ) -> Result<dataset::properties::segment::Properties, dataset::error::Properties> {
            let segment_path = dataset::utils::segment_path(segment);
            self.inner.segment_properties(segment_path)
        }

        pub fn channel_data(
            &mut self,
            segment: dataset::SegmentType,
            channel: impl AsRef<str>,
        ) -> Result<Vec<dataset::DataValue>, dataset::error::ChannelData> {
            let segment_path = dataset::utils::segment_path(segment);
            self.inner.channel_data(segment_path, channel)
        }
    }

    impl<R> Reader<R> {
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
        pub fn new(path: impl Into<PathBuf>) -> Result<Self, DatasetError> {
            let path = path.into();
            let file = fs::File::open(&path).map_err(|err| ::zip::result::ZipError::Io(err))?;
            let archive = ::zip::ZipArchive::new(file)?;
            let inner = Reader::new(archive)?;
            Ok(Self { path, inner })
        }

        pub fn path(&self) -> &PathBuf {
            &self.path
        }

        /// Loads data from all segments and all channels.
        pub fn load_data_all(&mut self) -> Result<pl::DataFrame, error::DataFile> {
            let segments_count = self.segments_count()?;
            if segments_count == 0 {
                return Ok(pl::DataFrame::empty());
            }

            let mut seg_cols = Vec::with_capacity(segments_count as usize);
            let mut headers = Vec::new();
            for segment in 0..segments_count {
                let properties = self.inner.segment_properties(segment)?;
                let channels = properties.channel_list()?;
                let mut scols = Vec::with_capacity(channels.len() + 1);
                for channel in channels {
                    let data = self.inner.channel_data(segment, channel)?;
                    let col = pl::Float64Chunked::from_vec(channel.into(), data).into_column();
                    scols.push(col);
                    headers.push(channel.to_string());
                }

                let length =
                    extract_value!(properties, SEGMENT_NUM_POINTS_PROPERTY_KEY, parse usize)?;
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
            Ok(pl::DataFrame::new(df)?)
        }
    }

    /// Read a collection of voltage spectroscopy files (`.jpk-voltage-ramp`) from a directory.
    #[derive(derive_more::Deref)]
    pub struct DirReader {
        path: PathBuf,
    }

    impl DirReader {
        pub fn new(path: impl Into<PathBuf>) -> Self {
            Self { path: path.into() }
        }

        pub fn load_data_all(&self) -> Result<pl::DataFrame, error::DataCollection> {
            let dir_walker = fs::read_dir(&self.path)?;
            let files = dir_walker
                .into_iter()
                .filter_map(|entry| entry.ok())
                .filter_map(|entry| {
                    let path = entry.path();
                    let ext = path.extension()?.to_str()?;
                    (path.is_file() && ext == VOLTAGE_SPECTROSCOPY_FILE_EXT).then_some(path)
                })
                .collect::<Vec<_>>();
            let readers = files
                .into_par_iter()
                .map(|path| {
                    FileReader::new(path.clone()).map_err(|err| error::DataCollection::Dataset {
                        path: path,
                        error: err,
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;

            let data = readers
                .into_par_iter()
                .map(|mut reader| {
                    let data =
                        reader
                            .load_data_all()
                            .map_err(|err| error::DataCollection::DataFile {
                                path: reader.path().clone(),
                                error: err,
                            })?;
                    let xy = reader
                        .position()
                        .map_err(|err| error::DataCollection::DataFile {
                            path: reader.path().clone(),
                            error: err.into(),
                        })?;

                    Result::<_, error::DataCollection>::Ok((xy, data))
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
    }

    pub mod error {
        use std::{io, path::PathBuf};

        use crate::dataset;

        #[derive(derive_more::From, Debug)]
        pub enum DataFile {
            Property(dataset::properties::error::Property),
            Properties(dataset::v2_0::error::Properties),
            ChannelData(dataset::v2_0::error::ChannelData),
            Polars(polars::error::PolarsError),
        }

        #[derive(derive_more::From, Debug)]
        pub enum DataCollection {
            Io(io::Error),
            Dataset {
                path: PathBuf,
                error: dataset::DatasetError,
            },
            DataFile {
                path: PathBuf,
                error: DataFile,
            },
        }
    }
}
