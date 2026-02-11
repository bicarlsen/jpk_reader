//! Load voltage spectroscopy data (`.jpk-voltage-spectroscopy`).

use pyo3::prelude::*;

#[pymodule(name = "voltage_spectroscopy")]
pub mod export {
    use jpk_reader::voltage_spectroscopy::v2_0 as jpk;
    use polars::prelude as pl;
    use pyo3::{exceptions::PyRuntimeError, prelude::*};
    use pyo3_polars::{
        PyDataFrame,
        export::polars_core::utils::rayon::iter::{IntoParallelIterator, ParallelIterator},
    };
    use std::{fs, path::PathBuf};

    /// Load a single voltage spectroscopy dataset (`.jpk-voltage-spectroscopy`).
    #[pyfunction]
    pub fn load_file(path: PathBuf) -> PyResult<PyDataFrame> {
        let mut reader = jpk::FileReader::new(path.clone()).map_err(|err| {
            PyRuntimeError::new_err(format!(
                "could not load data collection of {path:?}: {err:?}"
            ))
        })?;

        let mut df = reader.load_data_all().map_err(|err| {
            PyRuntimeError::new_err(format!(
                "could not load data of {:?}: {err:?}",
                reader.path()
            ))
        })?;

        let (x, y) = reader.position().map_err(|err| {
            PyRuntimeError::new_err(format!(
                "could not get xy position of {:?}: {err:?}",
                reader.path()
            ))
        })?;

        let length = df.height();
        let xcol = pl::Column::new_scalar(
            "x".into(),
            pl::Scalar::new(pl::DataType::Float64, x.into()),
            length,
        );
        let ycol = pl::Column::new_scalar(
            "y".into(),
            pl::Scalar::new(pl::DataType::Float64, y.into()),
            length,
        );

        df.with_column(xcol).unwrap();
        df.with_column(ycol).unwrap();

        Ok(PyDataFrame(df))
    }

    /// Load all `.jpk-voltage-spectroscopy` files within a directory.
    /// Does not recurse into children folders.
    #[pyfunction]
    pub fn load_dir(path: PathBuf) -> PyResult<PyDataFrame> {
        let reader = jpk::DirReader::new(path);
        let df = reader
            .load_data_all()
            .map_err(|err| PyRuntimeError::new_err(format!("could not load data: {err:?}")))?;
        Ok(PyDataFrame(df))
    }

    #[pyfunction]
    pub fn load_glob(pattern: String) -> PyResult<PyDataFrame> {
        todo!();
    }
}
