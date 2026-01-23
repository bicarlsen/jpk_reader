use arrow::{
    array::{
        ArrayRef, Float64Builder, ListBuilder, RecordBatch, StringBuilder, UInt8Builder,
        UInt32Builder,
    },
    datatypes::{DataType, Field, Schema},
    pyarrow::PyArrowType,
};
use jpk_reader::{self as jpk, qi_map::QIMapReader as _};
use pyo3::{exceptions::PyRuntimeError, prelude::*};
use std::{path::PathBuf, sync::Arc};

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

    fn all_data(&mut self) -> PyResult<PyArrowType<RecordBatch>> {
        self.inner
            .query_data(&jpk::qi_map::DataQuery::select_all())
            .map_err(|err| PyRuntimeError::new_err(err.to_string()))
            .map(|result| {
                let schema = Schema::new(vec![
                    Field::new("i", DataType::UInt32, false),
                    Field::new("j", DataType::UInt32, false),
                    Field::new("segment", DataType::UInt8, false),
                    Field::new("channel", DataType::Utf8, false),
                    Field::new("data", DataType::new_list(DataType::Float64, true), false), // TODO: Can List be non-nullable?
                ]);

                let (index, values) = result.into_parts();
                let mut idx_i = UInt32Builder::with_capacity(index.len());
                let mut idx_j = UInt32Builder::with_capacity(index.len());
                let mut idx_segment = UInt8Builder::with_capacity(index.len());
                let mut idx_channel =
                    StringBuilder::with_capacity(index.len(), index.len() * CHANNEL_NAME_LEN_HINT);
                for idx in index {
                    let jpk::qi_map::DataIndex {
                        pixel,
                        segment,
                        channel,
                    } = idx;
                    idx_i.append_value(pixel.i() as u32);
                    idx_j.append_value(pixel.j() as u32);
                    idx_segment.append_value(segment);
                    idx_channel.append_value(channel);
                }
                let idx_i = idx_i.finish();
                let idx_j = idx_j.finish();
                let idx_segment = idx_segment.finish();
                let idx_channel = idx_channel.finish();

                let mut value_builder =
                    ListBuilder::with_capacity(Float64Builder::new(), values.len());
                for data in values {
                    let data = data.into_iter().map(|value| Some(value));
                    value_builder.append_value(data);
                }
                let values = value_builder.finish();

                let schema = Arc::new(schema);
                let data = vec![
                    Arc::new(idx_i) as ArrayRef,
                    Arc::new(idx_j) as ArrayRef,
                    Arc::new(idx_segment) as ArrayRef,
                    Arc::new(idx_channel) as ArrayRef,
                    Arc::new(values) as ArrayRef,
                ];
                let records = RecordBatch::try_new(schema, data).unwrap();
                records.into()
            })
    }

    fn all_metadata(&mut self) -> PyResult {
        self.inner
            .query_metadata(&jpk_reader::qi_map::MetadataQuery::All)
            .map_err(|err| PyRuntimeError::new_err(err.to_string()))
    }

    // fn query_data(&mut self, index: jpk::qi_map::IndexQuery, segment: jpk::qi_map::SegmentQuery, channel: jpk::qi_map::ChannelQuery) ->
    // PyResult<PyArrowType<arrow::array::StructArray>> {
    // }
}
