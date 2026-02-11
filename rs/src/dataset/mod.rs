//! Reader for generic JPK datasets.
use std::{fmt, path::PathBuf};
pub mod properties;
pub mod v2_0;

const DATASET_PROPERTIES_FILE_PATH: &str = "header.properties";

#[derive(derive_more::From, Debug)]
pub enum DatasetError {
    #[from]
    OpenArchive(zip::result::ZipError),
    Zip {
        path: PathBuf,
        error: zip::result::ZipError,
    },
    InvalidFormat {
        path: PathBuf,
        cause: String,
    },
}

impl fmt::Display for DatasetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // TODO: Clean up error messages.
        write!(f, "{self:?}")
    }
}
