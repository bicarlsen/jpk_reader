//! Read various data produced by JPK AFM.

pub mod dataset;

// #[cfg(feature = "qi_map")]
// pub mod qi_map;
#[cfg(feature = "scope")]
pub mod scope;
#[cfg(feature = "voltage_spectroscopy")]
pub mod voltage_spectroscopy;

pub trait ArchiveReader {
    /// List of files in the archive.
    fn files(&self) -> Vec<&str>;

    /// Number of files in the archive.
    fn len(&self) -> usize;
}
