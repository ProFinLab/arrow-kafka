use std::sync::Arc;

use anyhow::Result;
use arrow::ffi_stream::ArrowArrayStreamReader;
use arrow::pyarrow::FromPyArrow;
use arrow::record_batch::RecordBatch;
use arrow_kafka::sink::ArrowKafkaSink as CoreSink;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

#[pyclass(name = "ArrowKafkaSink")]
pub struct ArrowKafkaSinkPy {
    inner: Arc<CoreSink>,
}

#[pymethods]
impl ArrowKafkaSinkPy {
    #[new]
    #[pyo3(signature = (
        kafka_servers,
        schema_registry_url,
        max_in_flight=1000,
        linger_ms=20,
        batch_size=65536,
        compression_type="none".to_string(),
    ))]
    pub fn new(
        kafka_servers: String,
        schema_registry_url: String,
        max_in_flight: usize,
        linger_ms: u32,
        batch_size: usize,
        compression_type: String,
    ) -> PyResult<Self> {
        let inner = CoreSink::new(
            kafka_servers,
            schema_registry_url,
            max_in_flight,
            linger_ms,
            batch_size,
            compression_type,
        )
        .map_err(to_pyerr)?;

        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    #[pyo3(signature = (table, topic, key_cols=None, key_separator="_".to_string(), timeout_ms=None))]
    pub fn consume_arrow(
        &self,
        py: Python<'_>,
        table: Py<PyAny>,
        topic: String,
        key_cols: Option<Vec<String>>,
        key_separator: String,
        timeout_ms: Option<u64>,
    ) -> PyResult<usize> {
        let key_cols = key_cols.unwrap_or_default();
        let sink = Arc::clone(&self.inner);

        py.detach(move || {
            consume_arrow_impl(&sink, &table, &topic, &key_cols, &key_separator, timeout_ms)
                .map_err(to_pyerr)
        })
    }

    #[pyo3(signature = (timeout_ms=30000))]
    pub fn flush(&self, py: Python<'_>, timeout_ms: u64) -> PyResult<usize> {
        let sink = Arc::clone(&self.inner);
        py.detach(move || sink.flush(timeout_ms).map_err(to_pyerr))
    }

    pub fn close(&self, py: Python<'_>) -> PyResult<()> {
        let sink = Arc::clone(&self.inner);
        py.detach(move || sink.close().map_err(to_pyerr))
    }
}

#[pymodule]
fn arrow_kafka_pyo3(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<ArrowKafkaSinkPy>()?;
    Ok(())
}

fn to_pyerr(e: anyhow::Error) -> PyErr {
    PyRuntimeError::new_err(format!("{e:#}"))
}

fn consume_arrow_impl(
    sink: &CoreSink,
    table: &Py<PyAny>,
    topic: &str,
    key_cols: &[String],
    key_separator: &str,
    _timeout_ms: Option<u64>,
) -> Result<usize> {
    let batches: Vec<RecordBatch> = Python::attach(|py| -> Result<_> {
        let bound = table.bind(py);
        let reader = ArrowArrayStreamReader::from_pyarrow_bound(bound)
            .map_err(|e| anyhow::anyhow!("PyArrow to Arrow stream error: {e}"))?;

        let mut out = Vec::new();
        for batch in reader {
            out.push(batch.map_err(|e| anyhow::anyhow!("Arrow stream read error: {e}"))?);
        }
        Ok(out)
    })?;

    if batches.is_empty() {
        return Ok(0);
    }

    let key_cols_opt: Option<&[String]> = if key_cols.is_empty() {
        None
    } else {
        Some(key_cols)
    };

    sink.consume_arrow(&batches, topic, key_cols_opt, key_separator)
}
