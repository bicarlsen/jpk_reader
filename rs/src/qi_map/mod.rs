//! QI force map data (`.jpk-qi-data`) reader.
use crate::dataset::properties::Properties;
use std::{
    cmp,
    collections::HashMap,
    fmt, fs, io, ops,
    path::{Path, PathBuf},
};

pub mod v2_0;

pub const QI_MAP_FILE_EXT: &str = "jpk-qi-data";

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

    ext == QI_MAP_FILE_EXT
}
