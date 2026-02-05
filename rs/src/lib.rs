use std::path::Path;
use zip;

pub mod properties;
pub mod qi_map;

pub trait ArchiveReader {
    /// List of files in the archive.
    fn files(&self) -> Vec<&str>;

    /// Number of files in the archive.
    fn len(&self) -> usize;
}
