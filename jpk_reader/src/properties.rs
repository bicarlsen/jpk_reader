use std::io::{self};

#[derive(Debug)]
pub struct Properties {
    /// Sorted list of properties.
    properties: Vec<(String, String)>,
}

impl Properties {
    pub fn new(reader: &mut impl io::Read) -> Result<Self, InvalidFormat> {
        let mut properties = std::collections::HashMap::new();
        let mut input = String::new();
        reader.read_to_string(&mut input).unwrap();
        for line in input.split_terminator("\n") {
            if line.starts_with("#") {
                continue;
            }
            let Some((key, value)) = line.split_once("=") else {
                return Err(InvalidFormat);
            };

            properties.insert(key.to_string(), value.to_string());
        }

        let mut properties = properties.into_iter().collect::<Vec<_>>();
        properties.sort_unstable_by_key(|(key, _)| key.clone());
        Ok(Self { properties })
    }

    /// Only extract keys matching those in `keys`.
    pub fn extract(
        reader: &mut impl io::Read,
        keys: &Vec<impl AsRef<str>>,
    ) -> Result<Self, InvalidFormat> {
        let keys = keys.iter().map(|key| key.as_ref()).collect::<Vec<_>>();
        let mut properties = std::collections::HashMap::with_capacity(keys.len());
        let mut input = String::new();
        reader.read_to_string(&mut input).unwrap();
        for line in input.split_terminator("\n") {
            if line.starts_with("#") {
                continue;
            }
            let Some((key, value)) = line.split_once("=") else {
                return Err(InvalidFormat);
            };

            if keys.contains(&key) {
                properties.insert(key.to_string(), value.to_string());
            }
        }

        let mut properties = properties.into_iter().collect::<Vec<_>>();
        properties.sort_unstable_by_key(|(key, _)| key.clone());
        Ok(Self { properties })
    }

    pub fn get(&self, key: impl AsRef<str>) -> Option<&String> {
        self.properties
            .binary_search_by_key(&key.as_ref(), |(key, _)| key)
            .ok()
            .map(|idx| &self.properties[idx].1)
    }
}

/// The format of the file was invalid.
pub struct InvalidFormat;

pub enum PropertyError {
    NotFound(String),
    /// The property with the given key had an invalid value.
    InvalidValue(String),
}

macro_rules! extract_value {
    ($props:ident, $key:expr) => {
        match $props.get($key) {
            None => Err(crate::properties::PropertyError::NotFound($key.to_string())),
            Some(value) => Ok(value),
        }
    };

    ($props:ident, $key:expr, parse $ty:ty) => {
        match $props.get($key) {
            None => Err(crate::properties::PropertyError::NotFound($key.to_string())),
            Some(value) => match value.parse::<$ty>() {
                Ok(value) => Ok(value),
                Err(_err) => Err(crate::properties::PropertyError::InvalidValue(
                    $key.to_string(),
                )),
            },
        }
    };

    ($props:ident, $key:expr, from_str $st:ty) => {
        match $props.get($key) {
            None => Err(crate::properties::PropertyError::NotFound($key.to_string())),
            Some(value) => match <$st>::from_str(value) {
                Some(value) => Ok(value),
                None => Err(crate::properties::PropertyError::InvalidValue(
                    $key.to_string(),
                )),
            },
        }
    };
}
pub(crate) use extract_value;
