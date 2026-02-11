//! Dataset reader for JPK file format version 2.0.

use super::{DatasetError, properties as dataset_properties};
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

const DATASET_PROPERTIES_SHARED_DATA_FILE_PATH: &str = "shared-data/header.properties";
const DATASET_SEGMENT_CHANNEL_DIR: &str = "channels";
const SEGMENT_PROPERTIES_FILE: &str = "segment-header.properties";

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
    pub fn new(mut archive: zip::ZipArchive<R>) -> Result<Self, DatasetError> {
        let dataset_properties = {
            let path = PathBuf::from(super::DATASET_PROPERTIES_FILE_PATH);
            let mut properties = archive.by_path(&path).map_err(|error| DatasetError::Zip {
                path: path.clone(),
                error,
            })?;

            let properties =
                dataset_properties::Properties::new(&mut properties).map_err(|_| {
                    DatasetError::InvalidFormat {
                        path: path.clone(),
                        cause: "invalid properties file".to_string(),
                    }
                })?;
            properties::Dataset { inner: properties }
        };

        let shared_properties = {
            let path = DATASET_PROPERTIES_SHARED_DATA_FILE_PATH;
            let mut properties = archive.by_path(&path).map_err(|error| DatasetError::Zip {
                path: PathBuf::from(path),
                error,
            })?;

            let properties =
                dataset_properties::Properties::new(&mut properties).map_err(|_err| {
                    DatasetError::InvalidFormat {
                        path: PathBuf::from(path),
                        cause: "invalid properties file".to_string(),
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
    ) -> Result<Vec<lcd_info::LcdInfo>, DatasetError> {
        let infos_count =
            dataset_properties::extract_value!(properties, properties::SharedData::LCD_INFOS_COUNT_KEY, parse usize)
                .map_err(|err| match err {
                    dataset_properties::error::Property::NotFound(key) => DatasetError::InvalidFormat {
                        path: PathBuf::from(DATASET_PROPERTIES_SHARED_DATA_FILE_PATH),
                        cause: format!(" property `{key}` not found",),
                    },
                    dataset_properties::error::Property::InvalidValue(key) => DatasetError::InvalidFormat {
                        path: PathBuf::from(DATASET_PROPERTIES_SHARED_DATA_FILE_PATH),
                        cause: format!("invalid value for property `{key}`",),
                    },
                })?;

        (0..infos_count)
            .map(|idx| Self::_init_lcd_info(properties, idx))
            .collect()
    }

    fn _init_lcd_info(
        properties: &properties::SharedData,
        index: usize,
    ) -> Result<lcd_info::LcdInfo, DatasetError> {
        lcd_info::LcdInfo::from_properties(properties, index).map_err(|err| match err {
            dataset_properties::error::Property::NotFound(key) => DatasetError::InvalidFormat {
                path: PathBuf::from(DATASET_PROPERTIES_SHARED_DATA_FILE_PATH),
                cause: format!("property `{key}` not found"),
            },
            dataset_properties::error::Property::InvalidValue(key) => DatasetError::InvalidFormat {
                path: PathBuf::from(DATASET_PROPERTIES_SHARED_DATA_FILE_PATH),
                cause: format!("invalid property value of `{key}`"),
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
    pub fn segment_properties(
        &mut self,
        segment_path: impl AsRef<Path>,
    ) -> Result<properties::segment::Properties, error::Properties> {
        let properties_path = segment_path.as_ref().join(SEGMENT_PROPERTIES_FILE);
        let mut file = self.archive.by_path(properties_path)?;
        let properties = dataset_properties::Properties::new(&mut file)?;
        Ok(properties::segment::Properties { inner: properties })
    }

    pub fn lcd_info_for_index(&self, index: LcdInfoIndexType) -> Option<&lcd_info::LcdInfo> {
        self.lcd_info.get(index as usize)
    }

    pub fn channel_info(
        &mut self,
        segment_path: impl AsRef<Path>,
        channel: impl AsRef<str>,
    ) -> Result<properties::channel::Info, error::ChannelInfo> {
        let channel = channel.as_ref();
        let properties = self.segment_properties(segment_path)?;
        let info = properties.channel_info(channel)?;
        Ok(info)
    }

    pub fn channel_data(
        &mut self,
        segment_path: impl AsRef<Path>,
        channel: impl AsRef<str>,
    ) -> Result<Vec<DataValue>, error::ChannelData> {
        let channel_info = self.channel_info(&segment_path, channel)?;
        let data_file_path = segment_path.as_ref().join(channel_info.file_path());
        let mut data_file = self.archive.by_path(&data_file_path)?;
        let mut raw_data = Vec::with_capacity(data_file.size() as usize);
        data_file
            .read_to_end(&mut raw_data)
            .map_err(|err| zip::result::ZipError::Io(err))?;
        drop(data_file);
        let lcd_info = self
            .lcd_info_for_index(channel_info.lcd_info_index())
            .expect("lcd info not found");
        let data = lcd_info.convert_data(&raw_data)?;
        Ok(data)
    }
}

impl<R> DatasetReader<R>
where
    R: io::Read + io::Seek,
{
    fn format_version(archive: &mut zip::ZipArchive<R>) -> Result<String, DatasetError> {
        let properties = {
            let mut properties = archive
                .by_path(super::DATASET_PROPERTIES_FILE_PATH)
                .map_err(|error| DatasetError::Zip {
                    path: PathBuf::from(super::DATASET_PROPERTIES_FILE_PATH),
                    error,
                })?;

            dataset_properties::Properties::new(&mut properties).map_err(|_err| {
                DatasetError::InvalidFormat {
                    path: PathBuf::from(super::DATASET_PROPERTIES_FILE_PATH),
                    cause: "invalid format".to_string(),
                }
            })?
        };

        let Some(format_version) = properties.get(properties::Dataset::FILE_FORMAT_VERSION_KEY)
        else {
            return Err(DatasetError::InvalidFormat {
                path: PathBuf::from(super::DATASET_PROPERTIES_FILE_PATH),
                cause: format!(
                    "property `{}` not found",
                    properties::Dataset::FILE_FORMAT_VERSION_KEY
                ),
            });
        };

        Ok(format_version.clone())
    }
}

pub mod utils {
    use super::{IndexType, SEGMENT_PROPERTIES_FILE, SegmentType};
    use std::path::PathBuf;

    pub const INDEX_DIR: &str = "index";
    pub const SEGMENT_DIR: &str = "segments";
    pub const DATASET_PROPERTIES_FILE: &str = "header.properties";
    pub const INDEX_PROPERTIES_FILE: &str = "header.properties";
    pub const SHARED_DATA_PROPERTIES_FILE: &str = "header.properties";
    pub const PROPERTIES_KEY_SEGMENT_CHANNELS_LIST: &str = "channels.list";

    pub fn segment_path(segment: SegmentType) -> PathBuf {
        let path = format!("{SEGMENT_DIR}/{segment}/");
        PathBuf::from(path)
    }

    pub fn index_properties_path(index: IndexType) -> PathBuf {
        let path = format!("{INDEX_DIR}/{index}/{INDEX_PROPERTIES_FILE}");
        PathBuf::from(path)
    }

    pub fn index_segment_path(index: IndexType, segment: SegmentType) -> PathBuf {
        let path = format!("{INDEX_DIR}/{index}/{SEGMENT_DIR}/{segment}/");
        PathBuf::from(path)
    }

    pub fn index_segment_properties_path(index: IndexType, segment: SegmentType) -> PathBuf {
        let path = format!("{INDEX_DIR}/{index}/{SEGMENT_DIR}/{segment}/{SEGMENT_PROPERTIES_FILE}");
        PathBuf::from(path)
    }
}

pub mod error {
    use super::{dataset_properties, lcd_info};

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

    #[derive(derive_more::From, Debug)]
    pub enum ChannelInfo {
        #[from]
        Zip(zip::result::ZipError),
        #[from]
        SegmentProperties(Properties),
        #[from]
        Property(dataset_properties::error::Property),
    }

    #[derive(derive_more::From, Debug)]
    pub enum ChannelData {
        #[from]
        Zip(zip::result::ZipError),
        #[from]
        SegmentProperties(Properties),
        #[from]
        Property(dataset_properties::error::Property),
        InvalidDataLength,
    }

    impl From<ChannelInfo> for ChannelData {
        fn from(value: ChannelInfo) -> Self {
            match value {
                ChannelInfo::Zip(err) => Self::Zip(err),
                ChannelInfo::Property(err) => Self::Property(err),
                ChannelInfo::SegmentProperties(err) => Self::SegmentProperties(err),
            }
        }
    }

    impl From<lcd_info::decoder::InvalidDataLength> for ChannelData {
        fn from(_value: lcd_info::decoder::InvalidDataLength) -> Self {
            Self::InvalidDataLength
        }
    }
}
