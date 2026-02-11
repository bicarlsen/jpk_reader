//! JPK version 2.0 specific properties.
use crate::dataset::properties as dataset_properties;

#[derive(derive_more::Deref)]
pub struct Dataset {
    pub(super) inner: dataset_properties::Properties,
}

impl Dataset {
    pub const DATASET_TYPE_KEY: &str = "type";
    pub const DATA_FILE_KEY: &str = "jpk-data-file";
    pub const FILE_FORMAT_VERSION_KEY: &str = "file-format-version";

    pub fn data_file(&self) -> Option<&String> {
        self.get(Self::DATA_FILE_KEY)
    }

    pub fn file_format_version(&self) -> Option<&String> {
        self.get(Self::FILE_FORMAT_VERSION_KEY)
    }

    pub fn dataset_type(&self) -> Option<&String> {
        self.get(Self::DATASET_TYPE_KEY)
    }
}

#[derive(derive_more::Deref)]
pub struct SharedData {
    pub(super) inner: dataset_properties::Properties,
}

impl SharedData {
    pub const LCD_INFOS_COUNT_KEY: &str = "lcd-infos.count";

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

pub mod index {
    use super::dataset_properties;
    use crate::dataset::v2_0::SegmentType;

    #[derive(derive_more::Deref)]
    pub struct IndexProperties {
        inner: dataset_properties::Properties,
    }

    pub struct IndexData {
        segment_count: SegmentType,
    }

    impl IndexData {
        const SEGMENT_COUNT_KEY: &str = "quantitative-imaging-series.force-segments.count";
    }

    impl IndexData {
        pub fn from_properties(
            properties: &dataset_properties::Properties,
        ) -> Result<Self, dataset_properties::error::Property> {
            let segment_count = dataset_properties::extract_value!(properties, Self::SEGMENT_COUNT_KEY, parse SegmentType)?;

            Ok(Self { segment_count })
        }
    }

    impl IndexData {
        pub fn segment_count(&self) -> SegmentType {
            self.segment_count
        }
    }
}

pub mod segment {
    use super::{channel, dataset_properties};
    use std::fmt;

    #[derive(derive_more::Deref)]
    pub struct Properties {
        pub(crate) inner: dataset_properties::Properties,
    }

    impl Properties {
        const CHANNELS_LIST_KEY: &str = "channels.list";

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
        pub fn channel_lcd_info_index_key(channel: impl fmt::Display) -> String {
            format!("channel.{channel}.lcd-info.*")
        }
    }

    impl Properties {
        pub fn channel_list(&self) -> Result<Vec<&str>, dataset_properties::error::Property> {
            let channels = dataset_properties::extract_value!(self, Self::CHANNELS_LIST_KEY)?;
            let list = channels.split(' ').collect::<Vec<_>>();
            Ok(list)
        }

        pub fn channel_info(
            &self,
            channel: impl fmt::Display,
        ) -> Result<channel::Info, dataset_properties::error::Property> {
            channel::Info::from(self, channel)
        }
    }
}

pub mod channel {
    use super::{super::LcdInfoIndexType, dataset_properties, segment};
    use std::{fmt, path::PathBuf};

    #[derive(Debug)]
    pub struct Info {
        file_path: PathBuf,
        file_format: DataFileFormat,
        num_points: u32,
        lcd_info_index: LcdInfoIndexType,
    }

    impl Info {
        #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
        pub fn from(
            properties: &segment::Properties,
            channel: impl fmt::Display,
        ) -> Result<Self, dataset_properties::error::Property> {
            let file_path = dataset_properties::extract_value!(
                properties,
                segment::Properties::channel_data_file_name_key(&channel)
            )?;
            let file_format = dataset_properties::extract_value!(properties,segment::Properties::channel_data_file_format_key(&channel), from_str DataFileFormat )?;
            let num_points = dataset_properties::extract_value!(properties, segment::Properties::channel_data_num_points_key(&channel), parse u32)?;
            let lcd_info_index = dataset_properties::extract_value!(properties, segment::Properties::channel_lcd_info_index_key(&channel), parse u8)?;

            let data = Self {
                file_path: PathBuf::from(file_path),
                file_format,
                num_points,
                lcd_info_index,
            };
            #[cfg(feature = "tracing")]
            tracing::trace!(?data);

            Ok(data)
        }
    }

    impl Info {
        pub fn file_path(&self) -> &PathBuf {
            &self.file_path
        }

        pub fn file_format(&self) -> DataFileFormat {
            self.file_format
        }
        pub fn num_points(&self) -> u32 {
            self.num_points
        }
        pub fn lcd_info_index(&self) -> LcdInfoIndexType {
            self.lcd_info_index
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
}
