use arrow::{
    array::{
        ArrayRef, Float64Builder, ListBuilder, RecordBatch, StringBuilder, UInt8Builder,
        UInt32Builder,
    },
    datatypes::{DataType, Field, Schema},
    pyarrow::PyArrowType,
};
use jpk_reader::{self as jpk, ArchiveReader, qi_map::QIMapReader as _};
use pyo3::{exceptions::PyRuntimeError, prelude::*, types::PyDict};
use std::{borrow::Cow, path::PathBuf, sync::Arc};

const CHANNEL_NAME_LEN_HINT: usize = 10;

/// Python exports
#[pymodule]
mod jpk_reader_rs {
    #[pymodule_export]
    use super::QIMapReader;
}

#[pyclass]
pub struct QIMapReader {
    inner: jpk::qi_map::VersionedFileReader,
}

#[pymethods]
impl QIMapReader {
    #[new]
    fn new(path: PathBuf) -> PyResult<Self> {
        let reader = jpk::qi_map::FileReader::new_versioned(path)
            .map_err(|err| PyRuntimeError::new_err(err.to_string()))?;
        Ok(Self { inner: reader })
    }

    fn len(&self) -> PyResult<usize> {
        Ok(self.inner.len())
    }

    fn files(&self) -> PyResult<Vec<&str>> {
        const SEPARATOR: &str = "/";
        let mut files = self.inner.files();
        let max_digits = files.len().ilog10() + 2;
        files.sort_by_key(|file| {
            let components = file
                .split(SEPARATOR)
                .map(|component| {
                    if let Ok(value) = component.parse::<usize>() {
                        Cow::Owned(format!("{:0>width$}", value, width = max_digits))
                    } else {
                        Cow::Borrowed(component)
                    }
                })
                .collect::<Vec<_>>();
            components.join(SEPARATOR)
        });

        return Ok(files);
    }

    fn all_data(&mut self) -> PyResult<PyArrowType<RecordBatch>> {
        let data = self
            .inner
            .query_data(&jpk::qi_map::DataQuery::select_all())
            .map_err(|err| PyRuntimeError::new_err(err.to_string()))?;

        let schema = Schema::new(vec![
            Field::new("index", DataType::UInt32, false),
            Field::new("segment", DataType::UInt8, false),
            Field::new("channel", DataType::Utf8, false),
            Field::new("data", DataType::new_list(DataType::Float64, true), false), // TODO: Can List be non-nullable?
        ]);

        let (index, values) = data.into_parts();
        let mut idx_index = UInt32Builder::with_capacity(index.len());
        let mut idx_segment = UInt8Builder::with_capacity(index.len());
        let mut idx_channel =
            StringBuilder::with_capacity(index.len(), index.len() * CHANNEL_NAME_LEN_HINT);
        for idx in index {
            let jpk::qi_map::DataIndex {
                index,
                segment,
                channel,
            } = idx;
            idx_index.append_value(index);
            idx_segment.append_value(segment);
            idx_channel.append_value(channel);
        }
        let idx_i = idx_index.finish();
        let idx_segment = idx_segment.finish();
        let idx_channel = idx_channel.finish();

        let mut value_builder = ListBuilder::with_capacity(Float64Builder::new(), values.len());
        for data in values {
            let data = data.into_iter().map(|value| Some(value));
            value_builder.append_value(data);
        }
        let values = value_builder.finish();

        let schema = Arc::new(schema);
        let data = vec![
            Arc::new(idx_i) as ArrayRef,
            Arc::new(idx_segment) as ArrayRef,
            Arc::new(idx_channel) as ArrayRef,
            Arc::new(values) as ArrayRef,
        ];
        let records = RecordBatch::try_new(schema, data).unwrap();
        Ok(records.into())
    }

    fn all_metadata(&mut self, py: Python) -> PyResult<Py<PyAny>> {
        let data = self
            .inner
            .query_metadata(&jpk_reader::qi_map::MetadataQuery::All)
            .map_err(|err| PyRuntimeError::new_err(err.to_string()))?;

        let dict = PyDict::new(py);
        for (index, properties) in data.into_iter() {
            let props = PyDict::new(py);
            for (key, value) in properties.into_iter() {
                props.set_item(key, value)?;
            }

            let mut idx = ("", None, None);
            match index {
                jpk_reader::qi_map::MetadataIndex::Dataset => {
                    idx.0 = "dataset";
                }
                jpk_reader::qi_map::MetadataIndex::SharedData => {
                    idx.0 = "shared_data";
                }
                jpk_reader::qi_map::MetadataIndex::Index(index) => {
                    idx.0 = "index";
                    idx.1 = Some(index);
                }
                jpk_reader::qi_map::MetadataIndex::Segment { index, segment } => {
                    idx.0 = "segment";
                    idx.1 = Some(index);
                    idx.2 = Some(segment);
                }
            };
            dict.set_item(idx, props)?;
        }

        Ok(dict.into())
    }
}
