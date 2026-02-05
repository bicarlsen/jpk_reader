use super::Value;
use crate::properties::{self, PropertyError};
use std::sync::Arc;

#[derive(Clone)]
pub struct LcdInfo {
    data_type: DataType,
    channel_info: ChannelInfo,
    unit: String,
    decoder: Arc<dyn decoder::Decode + Sync + Send>,
    conversion_set: conversion::ConversionSet,
}

impl LcdInfo {
    /// `lcd-info.{index}.unit.unit`
    fn unit_key(index: usize) -> String {
        format!("lcd-info.{index}.unit.unit")
    }

    /// `lcd-info.{index}.type`
    fn data_type_key(index: usize) -> String {
        format!("lcd-info.{index}.type")
    }
}

impl LcdInfo {
    pub fn from_properties(
        properties: &super::SharedData,
        index: usize,
    ) -> Result<Self, PropertyError> {
        let unit = properties::extract_value!(properties, Self::unit_key(index))?;
        let data_type =
            properties::extract_value!(properties, Self::data_type_key(index), from_str DataType)?;
        let decoder = match data_type {
            DataType::Integer => decoder::int_from_properties(&properties, index)?,
            DataType::Float => Arc::new(decoder::RawFloatDecoder),
            DataType::Raster => Arc::new(decoder::RasterDecoder),
        };

        let channel_info = ChannelInfo::from_properties(properties, index)?;
        let conversion_set = conversion::ConversionSet::from_properties(properties, index)?;

        Ok(Self {
            data_type,
            channel_info,
            unit: unit.clone(),
            decoder,
            conversion_set,
        })
    }
}

impl LcdInfo {
    /// # Returns
    /// List of available unit conversions.
    pub fn available_units(&self) -> Vec<String> {
        todo!("LcdInfo::available_units")
    }

    /// Convrt raw data to default units.
    pub fn convert_data(
        &self,
        data: &decoder::RawData,
    ) -> Result<Vec<Value>, decoder::InvalidDataLength> {
        let data = self.decoder.decode(data)?;
        let data = self.conversion_set.convert(data);
        Ok(data)
    }

    /// Get data in given units.
    pub fn convert_data_to(&self, unit: String) -> Result<Vec<Value>, PropertyError> {
        todo!("LcdInfo::convert_data_to")
    }
}

#[derive(Clone, Copy)]
pub enum DataType {
    /// Refer to encoder for concrete data type.
    Integer,
    /// `f32`.
    Float,
    Raster,
}

impl DataType {
    pub fn from_str(input: impl AsRef<str>) -> Option<Self> {
        match input.as_ref() {
            "integer-data" => Some(Self::Integer),
            "float-data" => Some(Self::Float),
            "raster-data" => Some(Self::Raster),
            _ => None,
        }
    }
}

#[derive(Clone)]
pub struct ChannelInfo {
    pub kind: ChannelKind,
    pub name: String,
    pub fancy_name: String,
}

impl ChannelInfo {
    /// `lcd-info.{index}.channel.type`
    fn type_key(index: usize) -> String {
        format!("lcd-info.{index}.channel.type")
    }

    /// `lcd-info.{index}.channel.name`
    fn name_key(index: usize) -> String {
        format!("lcd-info.{index}.channel.name")
    }

    /// `lcd-info.{index}.channel.fancy-name`
    fn fancy_name_key(index: usize) -> String {
        format!("lcd-info.{index}.channel.fancy-name")
    }
}

impl ChannelInfo {
    pub fn from_properties(
        properties: &super::SharedData,
        index: usize,
    ) -> Result<Self, PropertyError> {
        let kind =
            properties::extract_value!(properties, Self::type_key(index), from_str ChannelKind)?;
        let name = properties::extract_value!(properties, Self::name_key(index))?;
        let fancy_name = properties::extract_value!(properties, Self::fancy_name_key(index))?;

        Ok(Self {
            kind,
            name: name.clone(),
            fancy_name: fancy_name.clone(),
        })
    }
}

#[derive(Clone, Copy)]
pub enum ChannelKind {
    Channel,
}

impl ChannelKind {
    pub fn from_str(input: impl AsRef<str>) -> Option<Self> {
        match input.as_ref() {
            "channel" => Some(Self::Channel),
            _ => None,
        }
    }
}

pub mod scale {
    use super::Value;

    pub trait Scale<T> {
        /// Scale data.
        fn scale(&self, value: T) -> Value;
    }

    pub enum Type {
        Linear,
    }

    impl Type {
        pub fn from_str(input: impl AsRef<str>) -> Option<Self> {
            match input.as_ref() {
                "linear" => Some(Self::Linear),
                _ => None,
            }
        }
    }

    pub enum Style {
        OffsetMultiplier,
    }

    impl Style {
        pub fn from_str(input: impl AsRef<str>) -> Option<Self> {
            match input.as_ref() {
                "offsetmultiplier" => Some(Self::OffsetMultiplier),
                _ => None,
            }
        }
    }

    pub struct Identity;
    macro_rules! impl_scale_identity {
      ($($ty:ty),+) => {
                    $(impl Scale<$ty> for Identity {
                        fn scale(&self, value: $ty) -> Value {
                            Into::<Value>::into(value)
                        }
                    })+
                };
    }
    impl_scale_identity!(i16, u16, i32, u32);

    impl Scale<i64> for Identity {
        fn scale(&self, value: i64) -> Value {
            value as Value
        }
    }

    impl Scale<Value> for Identity {
        fn scale(&self, value: Value) -> Value {
            value
        }
    }

    pub struct LinearOffsetMultiplier {
        a0: Value,
        a1: Value,
    }

    impl LinearOffsetMultiplier {
        pub fn new(a0: Value, a1: Value) -> Self {
            Self { a0, a1 }
        }
    }

    macro_rules! impl_scale_linear_offset_multiplier {
        ($($ty:ty),+) => {
            $(impl Scale<$ty> for LinearOffsetMultiplier {
                fn scale(&self, value: $ty) -> Value {
                    self.a0 + self.a1 * Into::<Value>::into(value)
                }
            })+
        };
    }
    impl_scale_linear_offset_multiplier!(i16, u16, i32, u32);

    impl Scale<i64> for LinearOffsetMultiplier {
        fn scale(&self, value: i64) -> Value {
            self.a0 + self.a1 * value as Value
        }
    }

    impl Scale<Value> for LinearOffsetMultiplier {
        fn scale(&self, value: Value) -> Value {
            self.a0 + self.a1 * value
        }
    }
}

pub mod decoder {
    use super::{super::SharedData, Value, scale::Scale};
    use crate::properties;
    use std::{mem, sync::Arc};

    pub type RawData = [u8];

    pub trait Decode {
        fn decode(&self, data: &RawData) -> Result<Vec<Value>, InvalidDataLength>;
    }

    impl<T, D> Decode for T
    where
        T: DecodeRaw<Data = D> + Scale<D>,
    {
        fn decode(&self, data: &RawData) -> Result<Vec<Value>, InvalidDataLength> {
            let data = DecodeRaw::decode_raw(self, data)?;
            let data = data
                .into_iter()
                .map(|value| Scale::scale(self, value))
                .collect();
            Ok(data)
        }
    }

    pub trait DecodeRaw {
        type Data;

        fn decode_raw(&self, data: &RawData) -> Result<Vec<Self::Data>, InvalidDataLength>;
    }

    pub struct RawFloatDecoder;
    impl DecodeRaw for RawFloatDecoder {
        type Data = f32;
        fn decode_raw(&self, data: &RawData) -> Result<Vec<Self::Data>, InvalidDataLength> {
            let chunk_size = mem::size_of::<f32>();
            let mut chunks = data.chunks_exact(chunk_size);
            let data = chunks
                .by_ref()
                .map(|chunk| {
                    f32::from_be_bytes(chunk.try_into().expect("chunk has correct length"))
                })
                .collect::<Vec<_>>();
            if !chunks.remainder().is_empty() {
                return Err(InvalidDataLength);
            }

            Ok(data)
        }
    }

    impl Scale<f32> for RawFloatDecoder {
        fn scale(&self, value: f32) -> Value {
            return value as Value;
        }
    }

    pub struct IntDecoder<T> {
        scale: Arc<dyn Scale<T> + Sync + Send>,
        unit: String,
    }

    impl<T> IntDecoder<T> {
        pub fn new(scale: Arc<dyn Scale<T> + Sync + Send>, unit: String) -> Self {
            Self { scale, unit }
        }

        /// Size of the encoder's data type in bytes.
        pub fn data_type_size(&self) -> usize {
            mem::size_of::<T>()
        }
    }

    pub fn int_from_properties(
        properties: &SharedData,
        idx: usize,
    ) -> Result<Arc<dyn Decode + Sync + Send>, super::PropertyError> {
        use super::scale::{Style, Type};

        let data_type = properties::extract_value!(properties, SharedData::lcd_info_encoder_type_key(idx), from_str DataType)?;
        let scale_type = properties::extract_value!(properties, SharedData::lcd_info_encoder_scaling_type_key(idx), from_str super::scale::Type)?;
        let scale_style = properties::extract_value!(properties, SharedData::lcd_info_encoder_scaling_style_key(idx), from_str super::scale::Style)?;
        let unit =
            properties::extract_value!(properties, SharedData::lcd_info_encoder_unit_key(idx))?;
        let unit = unit.clone();

        let scale = match (scale_type, scale_style) {
            (Type::Linear, Style::OffsetMultiplier) => {
                _create_decoder_linear_offset_multiplier(properties, idx)?
            }
        };
        let scale = Arc::new(scale);

        match data_type {
            DataType::I16 => Ok(Arc::new(IntDecoder::<i16>::new(scale, unit))),
            DataType::U16 => Ok(Arc::new(IntDecoder::<u16>::new(scale, unit))),
            DataType::I32 => Ok(Arc::new(IntDecoder::<i32>::new(scale, unit))),
            DataType::U32 => Ok(Arc::new(IntDecoder::<u32>::new(scale, unit))),
            DataType::I64 => Ok(Arc::new(IntDecoder::<i64>::new(scale, unit))),
        }
    }

    fn _create_decoder_linear_offset_multiplier(
        properties: &SharedData,
        idx: usize,
    ) -> Result<super::scale::LinearOffsetMultiplier, super::PropertyError> {
        let multiplier_key = SharedData::lcd_info_encoder_scaling_multiplier_key(idx);
        let Some(multiplier) = properties.get(&multiplier_key) else {
            return Err(super::PropertyError::NotFound(multiplier_key.clone()));
        };
        let Ok(multiplier) = multiplier.parse::<f64>() else {
            return Err(super::PropertyError::InvalidValue(multiplier_key.clone()));
        };

        let offset_key = SharedData::lcd_info_encoder_scaling_offset_key(idx);
        let Some(offset) = properties.get(&offset_key) else {
            return Err(super::PropertyError::NotFound(offset_key.clone()));
        };
        let Ok(offset) = offset.parse::<f64>() else {
            return Err(super::PropertyError::InvalidValue(offset_key.clone()));
        };

        Ok(super::scale::LinearOffsetMultiplier::new(
            offset, multiplier,
        ))
    }

    macro_rules! impl_decode_raw_for {
        ($ty:ty) => {
            impl DecodeRaw for IntDecoder<$ty> {
                type Data = $ty;

                fn decode_raw(&self, data: &RawData) -> Result<Vec<Self::Data>, InvalidDataLength> {
                    let chunk_size = self.data_type_size() / mem::size_of::<u8>();
                    let mut chunks = data.chunks_exact(chunk_size);
                    let data = chunks
                        .by_ref()
                        .map(|chunk| {
                            <$ty>::from_be_bytes(
                                chunk.try_into().expect("chunk has correct length"),
                            )
                        })
                        .collect::<Vec<_>>();
                    if !chunks.remainder().is_empty() {
                        return Err(InvalidDataLength);
                    }

                    Ok(data)
                }
            }
        };
    }
    impl_decode_raw_for!(i16);
    impl_decode_raw_for!(u16);
    impl_decode_raw_for!(i32);
    impl_decode_raw_for!(u32);
    impl_decode_raw_for!(i64);

    impl<T> Scale<T> for IntDecoder<T> {
        fn scale(&self, value: T) -> Value {
            self.scale.scale(value)
        }
    }

    /// # See also
    /// [`afmformats.formats.fmt_jpk.jpk_data.load_dat_raw`](https://github.com/AFM-analysis/afmformats/blob/cd2e758d8bded88b60f34e0f09e1a766239ab7f6/afmformats/formats/fmt_jpk/jpk_data.py#L115)
    pub enum DataType {
        I16,
        U16,
        I32,
        U32,
        I64,
    }

    impl DataType {
        pub fn from_str(input: impl AsRef<str>) -> Option<Self> {
            match input.as_ref() {
                "signedshort" => Some(Self::I16),
                "unsignedshort" => Some(Self::U16),
                "signedinteger" => Some(Self::I32),
                "unsignedinteger" => Some(Self::U32),
                "signedlong" => Some(Self::I64),
                _ => None,
            }
        }
    }

    pub struct RasterDecoder;
    impl DecodeRaw for RasterDecoder {
        type Data = usize;

        fn decode_raw(&self, data: &RawData) -> Result<Vec<Self::Data>, InvalidDataLength> {
            todo!("RasterDecoder::decode_raw")
        }
    }

    impl Scale<usize> for RasterDecoder {
        fn scale(&self, value: usize) -> Value {
            todo!("RasterDecoder::scale")
        }
    }

    /// Data has an invalid number of bytes for the given type.
    pub struct InvalidDataLength;
}

mod conversion {
    use super::{
        super::{SharedData, lcd_info::scale::Scale},
        Value, scale,
    };
    use crate::properties;
    use std::{fmt, sync::Arc};

    #[derive(Clone)]
    pub struct ConversionSet {
        quantities: Vec<String>,
        base: String,
        default: String,
        conversions: Vec<Conversion>,
    }

    impl ConversionSet {
        /// `lcd-info.{index}.conversion-set.conversions.list`
        pub fn list_key(index: usize) -> String {
            format!("lcd-info.{index}.conversion-set.conversions.list")
        }

        /// `lcd-info.{index}.conversion-set.conversions.default`
        pub fn default_key(index: usize) -> String {
            format!("lcd-info.{index}.conversion-set.conversions.default")
        }

        /// `lcd-info.{index}.conversion-set.conversions.base`
        pub fn base_key(index: usize) -> String {
            format!("lcd-info.{index}.conversion-set.conversions.base")
        }
    }

    impl ConversionSet {
        pub fn from_properties(
            properties: &SharedData,
            index: usize,
        ) -> Result<Self, super::PropertyError> {
            let quantities = Self::_quantities(properties, index)?;
            let base = properties::extract_value!(properties, Self::base_key(index))?;
            let default = properties::extract_value!(properties, Self::default_key(index))?;
            let conversions = quantities
                .iter()
                .map(|conversion| Conversion::from_properties(properties, index, conversion))
                .collect::<Result<Vec<_>, _>>()?;

            Ok(Self {
                quantities,
                base: base.clone(),
                default: default.clone(),
                conversions,
            })
        }

        fn _quantities(
            properties: &SharedData,
            index: usize,
        ) -> Result<Vec<String>, super::PropertyError> {
            let value = properties::extract_value!(properties, Self::list_key(index))?;
            let value = value
                .split_ascii_whitespace()
                .map(|value| value.to_string())
                .collect();
            Ok(value)
        }
    }

    impl ConversionSet {
        /// Convert from the `base` to the `default` quantity.
        pub fn convert(&self, data: Vec<Value>) -> Vec<Value> {
            data.into_iter()
                .map(|mut value| {
                    for conversion in self.conversions.iter() {
                        value = conversion.scale(value);
                    }
                    value
                })
                .collect()
        }
    }

    #[derive(Clone)]
    pub struct Conversion {
        name: String,
        base_slot: Option<String>,
        calibration_slot: Option<String>,
        scale: Arc<dyn scale::Scale<Value> + Sync + Send>,
    }

    impl Conversion {
        /// `lcd-info.{index}.conversion-set.conversion.{conversion}.name`
        pub fn name_key(index: usize, conversion: impl fmt::Display) -> String {
            format!("lcd-info.{index}.conversion-set.conversion.{conversion}.name")
        }

        /// `lcd-info.{index}.conversion-set.conversion.{conversion}.defined`
        pub fn defined_key(index: usize, conversion: impl fmt::Display) -> String {
            format!("lcd-info.{index}.conversion-set.conversion.{conversion}.defined")
        }

        /// `lcd-info.{index}.conversion-set.conversion.{conversion}.base-calibration-slot`
        pub fn base_slot_key(index: usize, conversion: impl fmt::Display) -> String {
            format!("lcd-info.{index}.conversion-set.conversion.{conversion}.base-calibration-slot")
        }

        /// `lcd-info.{index}.conversion-set.conversion.{conversion}.calibration-slot`
        pub fn slot_key(index: usize, conversion: impl fmt::Display) -> String {
            format!("lcd-info.{index}.conversion-set.conversion.{conversion}.calibration-slot")
        }

        /// `lcd-info.{index}.conversion-set.conversion.{conversion}.scaling.type=linear`
        pub fn scaling_type_key(index: usize, conversion: impl fmt::Display) -> String {
            format!("lcd-info.{index}.conversion-set.conversion.{conversion}.scaling.type")
        }

        /// `lcd-info.{index}.conversion-set.conversion.{conversion}.scaling.style=offsetmultiplier`
        pub fn scaling_style_key(index: usize, conversion: impl fmt::Display) -> String {
            format!("lcd-info.{index}.conversion-set.conversion.{conversion}.scaling.style")
        }

        /// `lcd-info.{index}.conversion-set.conversion.{conversion}.scaling.offset=0.0`
        pub fn scaling_offset_key(index: usize, conversion: impl fmt::Display) -> String {
            format!("lcd-info.{index}.conversion-set.conversion.{conversion}.scaling.offset")
        }

        /// `lcd-info.{index}.conversion-set.conversion.{conversion}.scaling.multiplier=3.5381190902750224E-8`
        pub fn scaling_multiplier_key(index: usize, conversion: impl fmt::Display) -> String {
            format!("lcd-info.{index}.conversion-set.conversion.{conversion}.scaling.multiplier")
        }

        /// `lcd-info.{index}.conversion-set.conversion.{conversion}.scaling.unit.unit=m`
        pub fn scaling_unit_key(index: usize, conversion: impl fmt::Display) -> String {
            format!("lcd-info.{index}.conversion-set.conversion.{conversion}.scaling.unit.unit")
        }
    }

    impl Conversion {
        pub fn from_properties(
            properties: &SharedData,
            index: usize,
            conversion: impl fmt::Display,
        ) -> Result<Self, super::PropertyError> {
            let defined = properties::extract_value!(properties, Self::defined_key(index, &conversion), parse bool)?;
            let name = properties::extract_value!(properties, Self::name_key(index, &conversion))?;
            if !defined {
                return Ok(Self {
                    name: name.clone(),
                    base_slot: None,
                    calibration_slot: None,
                    scale: Arc::new(scale::Identity),
                });
            }

            let base_slot =
                properties::extract_value!(properties, Self::base_slot_key(index, &conversion))?;
            let calibration_slot =
                properties::extract_value!(properties, Self::slot_key(index, &conversion))?;

            let scale_type = properties::extract_value!(properties, Self::scaling_type_key(index, &conversion), from_str super::scale::Type)?;
            let scale_style = properties::extract_value!(properties, Self::scaling_style_key(index, &conversion), from_str super::scale::Style)?;
            let scale = match (scale_type, scale_style) {
                (scale::Type::Linear, scale::Style::OffsetMultiplier) => {
                    Self::_create_linear_offset_multiplier(properties, index, &conversion)?
                }
            };
            let scale = Arc::new(scale);

            Ok(Self {
                name: name.clone(),
                base_slot: Some(base_slot.clone()),
                calibration_slot: Some(calibration_slot.clone()),
                scale,
            })
        }

        fn _create_linear_offset_multiplier(
            properties: &SharedData,
            idx: usize,
            conversion: impl fmt::Display,
        ) -> Result<super::scale::LinearOffsetMultiplier, super::PropertyError> {
            let multiplier_key = Self::scaling_multiplier_key(idx, &conversion);
            let Some(multiplier) = properties.get(&multiplier_key) else {
                return Err(super::PropertyError::NotFound(multiplier_key.clone()));
            };
            let Ok(multiplier) = multiplier.parse::<f64>() else {
                return Err(super::PropertyError::InvalidValue(multiplier_key.clone()));
            };

            let offset_key = Self::scaling_offset_key(idx, &conversion);
            let Some(offset) = properties.get(&offset_key) else {
                return Err(super::PropertyError::NotFound(offset_key.clone()));
            };
            let Ok(offset) = offset.parse::<f64>() else {
                return Err(super::PropertyError::InvalidValue(offset_key.clone()));
            };

            Ok(scale::LinearOffsetMultiplier::new(offset, multiplier))
        }
    }

    impl scale::Scale<Value> for Conversion {
        fn scale(&self, value: Value) -> Value {
            self.scale.scale(value)
        }
    }
}
