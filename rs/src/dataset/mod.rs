//! Reader for generic JPK datasets backed by a zip archive.

use std::{fs, io, path::Path};

use crate::{qi_map, voltage_spectroscopy};
pub mod properties;
pub mod v2_0;

pub const DATASET_PROPERTIES_FILE_PATH: &str = "header.properties";

/// JPK file format versions.
pub enum FormatVersion {
    // 2.0
    V2_0,
}

impl FormatVersion {
    pub fn from_str(v: impl AsRef<str>) -> Option<FormatVersion> {
        match v.as_ref() {
            "2.0" => Some(Self::V2_0),
            _ => None,
        }
    }
}

pub fn format_version<R>(
    archive: &mut zip::ZipArchive<R>,
) -> Result<Option<FormatVersion>, error::Dataset>
where
    R: io::Read + io::Seek,
{
    let mut file = archive
        .by_path(DATASET_PROPERTIES_FILE_PATH)
        .map_err(|err| error::Dataset::Zip(err))?;
    let properties = properties::Properties::extract(
        &mut file,
        &vec![properties::DATASET_FILE_FORMAT_VERSION_KEY],
    )
    .map_err(|_| error::Dataset::InvalidFormat {
        cause: "could not get file format".into(),
    })?;
    let version = properties
        .get(properties::DATASET_FILE_FORMAT_VERSION_KEY)
        .unwrap();
    let version = FormatVersion::from_str(version);
    Ok(version)
}

/// Type of dataset.
#[derive(Clone, Copy, Debug)]
pub enum DatasetType {
    /// Single voltage spectroscopy datset.
    VoltageSpectroscopy,
    /// A collection of voltage spectroscopy datasets.
    VoltageSpectroscopyCollection,
    QIMap,
}

impl DatasetType {
    /// Guess the dataset type given the file system resource at path.
    ///
    /// e.g. If the path is a file ending in `.jpk-voltage-ramp` this will guess it
    /// is a `VoltageSpectroscopy` dataset.
    /// If the path is a directory containing only `.jpk-voltage-ramp` files, this will guess
    /// if is a `VoltageSpectroscopyCollection` dataset.
    pub fn from_fs(path: impl AsRef<Path>) -> Result<Option<DatasetType>, io::Error> {
        let path = path.as_ref();
        if path.is_file() {
            if voltage_spectroscopy::validate_path(&path) {
                return Ok(Some(DatasetType::VoltageSpectroscopy));
            } else if qi_map::validate_path(&path) {
                return Ok(Some(DatasetType::QIMap));
            } else {
                return Ok(None);
            }
        } else if path.is_dir() {
            let dir_walker = fs::read_dir(&path)?;
            let mut maybe_voltage_spectroscopy_collection = true;
            for file in dir_walker {
                let file = file?;
                if !voltage_spectroscopy::validate_path(file.path()) {
                    maybe_voltage_spectroscopy_collection = false;
                }
            }
            if maybe_voltage_spectroscopy_collection {
                return Ok(Some(DatasetType::VoltageSpectroscopyCollection));
            } else {
                return Ok(None);
            }
        } else {
            return Ok(None);
        }
    }
}

pub mod error {
    use std::path::PathBuf;

    #[derive(Debug)]
    pub struct Error<T> {
        pub paths: Vec<PathBuf>,
        pub error: T,
    }

    impl<T> Error<T> {
        pub fn new(error: T) -> Self {
            Self {
                paths: vec![],
                error,
            }
        }

        pub fn map_err<U, O>(self, op: O) -> Error<U>
        where
            O: FnOnce(T) -> U,
        {
            Error {
                paths: self.paths,
                error: op(self.error),
            }
        }
    }

    #[derive(Debug)]
    pub enum Dataset {
        OpenArchive(zip::result::ZipError),
        Zip(zip::result::ZipError),
        InvalidFormat { cause: String },
    }
}
