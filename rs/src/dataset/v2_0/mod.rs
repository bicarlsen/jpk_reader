//! Dataset reader for JPK file format version 2.0.

use std::{
    io::{self, Read},
    path::{Path, PathBuf},
    sync::Arc,
};

pub mod lcd_info;
pub mod properties;

pub type DataValue = f64;
pub type IndexType = u32;
pub type SegmentType = u8;
pub type LcdInfoIndexType = u8;

pub const PROPERTIES_SHARED_DATA_FILE_PATH: &str = "shared-data/header.properties";
pub const INDEX_DIR: &str = "index";
pub const SEGMENT_DIR: &str = "segments";
pub const INDEX_PROPERTIES_FILE: &str = "header.properties";
pub const SEGMENT_PROPERTIES_FILE: &str = "segment-header.properties";
pub const SEGMENT_CHANNEL_DIR: &str = "channels";

pub struct DatasetReader<R> {
    archive: zip::ZipArchive<R>,
    dataset_properties: Arc<properties::Dataset>,
    shared_properties: Arc<properties::SharedData>,
    lcd_info: Arc<Vec<lcd_info::LcdInfo>>,
}

impl<R> DatasetReader<R>
where
    R: io::Read + io::Seek,
{
    pub fn new(
        mut archive: zip::ZipArchive<R>,
    ) -> Result<Self, super::error::Error<super::error::Dataset>> {
        let dataset_properties = {
            let mut properties = archive
                .by_path(super::DATASET_PROPERTIES_FILE_PATH)
                .map_err(|err| super::error::Error {
                    paths: vec![PathBuf::from(super::DATASET_PROPERTIES_FILE_PATH)],
                    error: super::error::Dataset::Zip(err),
                })?;

            let properties = super::properties::Properties::new(&mut properties).map_err(|_| {
                super::error::Error {
                    paths: vec![PathBuf::from(super::DATASET_PROPERTIES_FILE_PATH)],
                    error: super::error::Dataset::InvalidFormat {
                        cause: "invalid properties file".to_string(),
                    },
                }
            })?;
            properties::Dataset { inner: properties }
        };

        let shared_properties = {
            let mut properties =
                archive
                    .by_path(PROPERTIES_SHARED_DATA_FILE_PATH)
                    .map_err(|err| super::error::Error {
                        paths: vec![PathBuf::from(PROPERTIES_SHARED_DATA_FILE_PATH)],
                        error: super::error::Dataset::Zip(err),
                    })?;

            let properties =
                super::properties::Properties::new(&mut properties).map_err(|_err| {
                    super::error::Error {
                        paths: vec![PathBuf::from(PROPERTIES_SHARED_DATA_FILE_PATH)],
                        error: super::error::Dataset::InvalidFormat {
                            cause: "invalid properties file".to_string(),
                        },
                    }
                })?;
            properties::SharedData { inner: properties }
        };
        let lcd_info = Self::_init_lcd_infos(&shared_properties)?;

        Ok(Self {
            archive,
            dataset_properties: Arc::new(dataset_properties),
            shared_properties: Arc::new(shared_properties),
            lcd_info: Arc::new(lcd_info),
        })
    }

    fn _init_lcd_infos(
        properties: &properties::SharedData,
    ) -> Result<Vec<lcd_info::LcdInfo>, super::error::Error<super::error::Dataset>> {
        let infos_count =
            super::properties::extract_value!(properties, properties::SharedData::LCD_INFOS_COUNT_KEY, parse usize)
                .map_err(|err| match err {
                    super::properties::error::Property::NotFound(key) => super::error::Error{
                        paths: vec![PathBuf::from(PROPERTIES_SHARED_DATA_FILE_PATH)],
                        error: super::error::Dataset::InvalidFormat {
                        cause: format!(" property `{key}` not found",),}
                    },
                    super::properties::error::Property::InvalidValue(key) => super::error::Error{
                        paths: vec![PathBuf::from(PROPERTIES_SHARED_DATA_FILE_PATH)],
                        error: super::error::Dataset::InvalidFormat {
                        cause: format!("invalid value for property `{key}`",),}
                    },
                })?;

        (0..infos_count)
            .map(|idx| Self::_init_lcd_info(properties, idx))
            .collect()
    }

    fn _init_lcd_info(
        properties: &properties::SharedData,
        index: usize,
    ) -> Result<lcd_info::LcdInfo, super::error::Error<super::error::Dataset>> {
        lcd_info::LcdInfo::from_properties(properties, index).map_err(|err| match err {
            super::properties::error::Property::NotFound(key) => super::error::Error {
                paths: vec![PathBuf::from(PROPERTIES_SHARED_DATA_FILE_PATH)],
                error: super::error::Dataset::InvalidFormat {
                    cause: format!("property `{key}` not found"),
                },
            },
            super::properties::error::Property::InvalidValue(key) => super::error::Error {
                paths: vec![PathBuf::from(PROPERTIES_SHARED_DATA_FILE_PATH)],
                error: super::error::Dataset::InvalidFormat {
                    cause: format!("invalid property value of `{key}`"),
                },
            },
        })
    }
}

impl<R> DatasetReader<R> {
    pub fn dataset_properties(&self) -> &Arc<properties::Dataset> {
        &self.dataset_properties
    }

    pub fn shared_properties(&self) -> &Arc<properties::SharedData> {
        &self.shared_properties
    }
}

impl<R> DatasetReader<R>
where
    R: io::Read + io::Seek,
{
    /// Loads a properties file.
    ///
    /// # Returns
    /// `Err((properties path, error))`
    pub fn segment_properties(
        &mut self,
        segment_path: impl AsRef<Path>,
    ) -> Result<properties::segment::Properties, super::error::Error<error::Properties>> {
        let properties_path = segment_path.as_ref().join(SEGMENT_PROPERTIES_FILE);
        let mut file =
            self.archive
                .by_path(&properties_path)
                .map_err(|err| super::error::Error {
                    paths: vec![properties_path.clone()],
                    error: err.into(),
                })?;
        let properties =
            super::properties::Properties::new(&mut file).map_err(|err| super::error::Error {
                paths: vec![properties_path.clone()],
                error: err.into(),
            })?;
        Ok(properties::segment::Properties {
            inner: properties,
            path: properties_path,
        })
    }

    pub fn get_lcd_info(&self, index: LcdInfoIndexType) -> Option<&lcd_info::LcdInfo> {
        self.lcd_info.get(index as usize)
    }

    // /// # Notes
    // /// + Loads the segment's properties file.
    // pub fn channel_info(
    //     &mut self,
    //     segment_path: impl AsRef<Path>,
    //     channel: impl AsRef<str>,
    // ) -> Result<properties::channel::Info, error::ChannelInfo> {
    //     let channel = channel.as_ref();
    //     let properties = self.segment_properties(segment_path)?;
    //     let info = properties.channel_info(channel)?;
    //     Ok(info)
    // }

    /// Get data for the given segment channel.
    /// Data is converted from raw values using lcd info.
    ///
    /// # Notes
    /// + Loads channel data file.
    pub fn channel_data(
        &mut self,
        segment_path: impl AsRef<Path>,
        channel_info: &properties::channel::Info,
    ) -> Result<Vec<DataValue>, crate::dataset::error::Error<error::ChannelData>> {
        let data_file_path = segment_path.as_ref().join(channel_info.file_path());
        let mut data_file =
            self.archive
                .by_path(&data_file_path)
                .map_err(|err| crate::dataset::error::Error {
                    paths: vec![data_file_path.clone()],
                    error: err.into(),
                })?;
        let mut raw_data = Vec::with_capacity(data_file.size() as usize);
        data_file
            .read_to_end(&mut raw_data)
            .map_err(|err| crate::dataset::error::Error {
                paths: vec![data_file_path.clone()],
                error: zip::result::ZipError::Io(err).into(),
            })?;
        drop(data_file);

        let Some(lcd_info) = self.get_lcd_info(channel_info.lcd_info_index()) else {
            return Err(crate::dataset::error::Error::new(
                error::ChannelData::NoLcdInfo,
            ));
        };
        let data =
            lcd_info
                .convert_data(&raw_data)
                .map_err(|err| crate::dataset::error::Error {
                    paths: vec![data_file_path.clone()],
                    error: err.into(),
                })?;
        Ok(data)
    }

    /// Get the JPK file format version from the dataset's properties.
    ///
    /// # Notes
    /// + Loads the dataset's properties.
    fn format_version(
        archive: &mut zip::ZipArchive<R>,
    ) -> Result<String, super::error::Error<super::error::Dataset>> {
        let properties = {
            let mut properties = archive
                .by_path(super::DATASET_PROPERTIES_FILE_PATH)
                .map_err(|error| super::error::Error {
                    paths: vec![PathBuf::from(super::DATASET_PROPERTIES_FILE_PATH)],
                    error: super::error::Dataset::Zip(error),
                })?;

            super::properties::Properties::new(&mut properties).map_err(|_| {
                super::error::Error {
                    paths: vec![PathBuf::from(super::DATASET_PROPERTIES_FILE_PATH)],
                    error: super::error::Dataset::InvalidFormat {
                        cause: "invalid format".to_string(),
                    },
                }
            })?
        };

        let Some(format_version) =
            properties.get(crate::dataset::properties::DATASET_FILE_FORMAT_VERSION_KEY)
        else {
            return Err(super::error::Error {
                paths: vec![PathBuf::from(super::DATASET_PROPERTIES_FILE_PATH)],
                error: super::error::Dataset::InvalidFormat {
                    cause: format!(
                        "property `{}` not found",
                        crate::dataset::properties::DATASET_FILE_FORMAT_VERSION_KEY
                    ),
                },
            });
        };

        Ok(format_version.clone())
    }
}

impl<R> crate::ArchiveReader for DatasetReader<R>
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

pub mod utils {
    use super::{INDEX_DIR, INDEX_PROPERTIES_FILE, IndexType, SEGMENT_DIR, SegmentType};
    use crate::dataset::v2_0::SEGMENT_PROPERTIES_FILE;

    #[inline]
    pub fn segment_path(segment: SegmentType) -> String {
        format!("{SEGMENT_DIR}/{segment}/")
    }

    #[inline]
    pub fn index_path(index: IndexType) -> String {
        format!("{INDEX_DIR}/{index}/")
    }

    #[inline]
    pub fn index_segment_path(index: IndexType, segment: SegmentType) -> String {
        format!("{}{}", index_path(index), segment_path(segment))
    }

    #[inline]
    pub fn index_properties_path(index: IndexType) -> String {
        format!("{}{}", index_path(index), INDEX_PROPERTIES_FILE)
    }

    #[inline]
    pub fn index_segment_properties_path(index: IndexType, segment: SegmentType) -> String {
        format!(
            "{}{}",
            index_segment_path(index, segment),
            SEGMENT_PROPERTIES_FILE,
        )
    }
}

pub mod error {
    use super::{super::properties as dataset_properties, lcd_info};
    use polars::prelude as pl;
    use std::path::PathBuf;

    /// Error while loading properties.
    #[derive(Debug, derive_more::From)]
    pub enum Properties {
        #[from]
        Zip(zip::result::ZipError),
        InvalidFormat,
    }

    impl From<dataset_properties::error::InvalidFormat> for Properties {
        fn from(_value: dataset_properties::error::InvalidFormat) -> Self {
            Self::InvalidFormat
        }
    }

    /// Error while loading channel info from properties.
    #[derive(derive_more::From, Debug)]
    pub enum ChannelInfo {
        #[from]
        Zip(zip::result::ZipError),
        #[from]
        SegmentProperties(Properties),
        #[from]
        Property(dataset_properties::error::Property),
    }

    /// Error while loading channel data.
    #[derive(derive_more::From, Debug)]
    pub enum ChannelData {
        #[from]
        Zip(zip::result::ZipError),
        /// Lcd info for the channel was not found.
        NoLcdInfo,
        /// Data file length does not match data type.
        InvalidDataLength,
    }

    impl From<lcd_info::decoder::InvalidDataLength> for ChannelData {
        fn from(_value: lcd_info::decoder::InvalidDataLength) -> Self {
            Self::InvalidDataLength
        }
    }

    /// Error while loading a segment's channel data.
    #[derive(derive_more::From, Debug)]
    pub enum SegmentChannelData {
        SegmentProperties(Properties),
        Property(dataset_properties::error::Property),
        ChannelData(ChannelData),
    }

    impl From<ChannelInfo> for SegmentChannelData {
        fn from(value: ChannelInfo) -> Self {
            match value {
                ChannelInfo::Zip(err) => Self::SegmentProperties(Properties::Zip(err)),
                ChannelInfo::Property(err) => Self::Property(err),
                ChannelInfo::SegmentProperties(err) => Self::SegmentProperties(err),
            }
        }
    }

    /// Error while loading mutliple channel data.
    #[derive(derive_more::From)]
    pub enum CollectionData {
        SegmentChannelData(SegmentChannelData),
        Polars(pl::PolarsError),
    }
}
