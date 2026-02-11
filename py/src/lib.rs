use pyo3::prelude::*;

// mod qi_map;
pub mod scope;
pub mod voltage_spectroscopy;

/// Python exports
#[pymodule]
mod jpk_reader_rs {
    // #[pymodule_export(name = "qi_map")]
    // use super::qi_map::export as qi_map;

    #[pymodule_export(name = "scope")]
    use super::scope::export as scope;

    #[pymodule_export(name = "voltage_spectroscopy")]
    use super::voltage_spectroscopy::export as voltage_spectroscopy;
}
