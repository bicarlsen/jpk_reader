//! Dataset properties.
use std::io::{self};

#[derive(Debug)]
pub struct Properties {
    /// Sorted list of properties.
    inner: Vec<(String, String)>,
}

impl Properties {
    pub fn new(reader: &mut impl io::Read) -> Result<Self, error::InvalidFormat> {
        let mut properties = std::collections::HashMap::new();
        let mut input = String::new();
        reader.read_to_string(&mut input).unwrap();
        for line in input.split_terminator("\n") {
            if line.starts_with("#") {
                continue;
            }
            let Some((key, value)) = line.split_once("=") else {
                return Err(error::InvalidFormat);
            };

            properties.insert(key.to_string(), value.to_string());
        }

        let mut properties = properties.into_iter().collect::<Vec<_>>();
        properties.sort_unstable_by_key(|(key, _)| key.clone());
        Ok(Self { inner: properties })
    }

    /// Only extract keys matching those in `keys`.
    pub fn extract(
        reader: &mut impl io::Read,
        keys: &Vec<impl AsRef<str>>,
    ) -> Result<Self, error::InvalidFormat> {
        let keys = keys.iter().map(|key| key.as_ref()).collect::<Vec<_>>();
        let mut properties = std::collections::HashMap::with_capacity(keys.len());
        let mut input = String::new();
        reader.read_to_string(&mut input).unwrap();
        for line in input.split_terminator("\n") {
            if line.starts_with("#") {
                continue;
            }
            let Some((key, value)) = line.split_once("=") else {
                return Err(error::InvalidFormat);
            };

            if keys.contains(&key) {
                properties.insert(key.to_string(), value.to_string());
            }
        }

        let mut properties = properties.into_iter().collect::<Vec<_>>();
        properties.sort_unstable_by_key(|(key, _)| key.clone());
        Ok(Self { inner: properties })
    }

    pub fn get(&self, key: impl AsRef<str>) -> Option<&String> {
        self.inner
            .binary_search_by_key(&key.as_ref(), |(key, _)| key)
            .ok()
            .map(|idx| &self.inner[idx].1)
    }
}

impl IntoIterator for Properties {
    type Item = (String, String);
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.into_iter()
    }
}

pub mod error {
    /// The format of the file was invalid.
    #[derive(Debug)]
    pub struct InvalidFormat;

    #[derive(Debug)]
    pub enum Property {
        NotFound(String),
        /// The property with the given key had an invalid value.
        InvalidValue(String),
    }
}

/// Extract a value from `Properties`, returning the relevant error.
///
/// # Returns
/// `Result<T, [crate::dataset::properties::error::Property]>`
/// where `T` is an `&String` if no type is given, otherwise it is the given type.
///
/// # Examples
/// let value_string = extract_value!(properties, "string_value_key");
/// let float_value = extract_value!(properties, "float_value_key", parse f64);
macro_rules! extract_value {
    ($props:expr, $key:expr) => {
        match $props.get($key) {
            None => Err(crate::dataset::properties::error::Property::NotFound(
                $key.to_string(),
            )),
            Some(value) => Ok(value),
        }
    };

    ($props:expr, $key:expr, parse $ty:ty) => {
        match $props.get($key) {
            None => Err(crate::dataset::properties::error::Property::NotFound(
                $key.to_string(),
            )),
            Some(value) => match value.parse::<$ty>() {
                Ok(value) => Ok(value),
                Err(_err) => Err(crate::dataset::properties::error::Property::InvalidValue(
                    $key.to_string(),
                )),
            },
        }
    };

    ($props:expr, $key:expr, from_str $st:ty) => {
        match $props.get($key) {
            None => Err(crate::dataset::properties::error::Property::NotFound(
                $key.to_string(),
            )),
            Some(value) => match <$st>::from_str(value) {
                Some(value) => Ok(value),
                None => Err(crate::dataset::properties::error::Property::InvalidValue(
                    $key.to_string(),
                )),
            },
        }
    };
}
pub(crate) use extract_value;
