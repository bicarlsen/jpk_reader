//! Load data from the real time oscilliscope (`.out`).

use pyo3::prelude::*;

#[pymodule(name = "scope")]
pub mod export {
    use jpk_reader::scope;
    use pyo3::{exceptions::PyRuntimeError, prelude::*};
    use pyo3_polars::PyDataFrame;
    use std::path::PathBuf;

    #[pyfunction]
    pub fn load_data(path: PathBuf) -> PyResult<PyDataFrame> {
        let loader =
            scope::load_data(&path).map_err(|err| PyRuntimeError::new_err(err.to_string()))?;
        let df = loader
            .collect()
            .map_err(|err| PyRuntimeError::new_err(err.to_string()))?;

        Ok(PyDataFrame(df))
    }
}
