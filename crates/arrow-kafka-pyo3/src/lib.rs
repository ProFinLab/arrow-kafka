use std::sync::Arc;

use arrow::ffi_stream::ArrowArrayStreamReader;
use arrow::pyarrow::FromPyArrow;
use arrow::record_batch::RecordBatch;
use arrow_kafka::admin;
use arrow_kafka::error::SinkError;
use arrow_kafka::schema_registry::SubjectNameStrategy;
use arrow_kafka::sink::ArrowKafkaSink as CoreSink;
use arrow_kafka::stats::SinkStats;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

// ---------------------------------------------------------------------------
// Structured Python exception hierarchy
//
//   ArrowKafkaError (base, extends RuntimeError)
//   ├── SchemaRegistryError
//   ├── SerializationError
//   ├── EnqueueError          — e.args[1] = enqueued_so_far (int)
//   ├── FlushTimeoutError
//   ├── UnsupportedTypeError
//   ├── ConfigError
//   └── AdminError
//
// All subclasses extend ArrowKafkaError so callers can catch the base class
// for a catch-all, or a specific subclass for targeted handling.
// ---------------------------------------------------------------------------

pyo3::create_exception!(arrow_kafka_pyo3, ArrowKafkaError, PyRuntimeError);
pyo3::create_exception!(arrow_kafka_pyo3, SchemaRegistryError, ArrowKafkaError);
pyo3::create_exception!(arrow_kafka_pyo3, SerializationError, ArrowKafkaError);
pyo3::create_exception!(arrow_kafka_pyo3, EnqueueError, ArrowKafkaError);
pyo3::create_exception!(arrow_kafka_pyo3, FlushTimeoutError, ArrowKafkaError);
pyo3::create_exception!(arrow_kafka_pyo3, UnsupportedTypeError, ArrowKafkaError);
pyo3::create_exception!(arrow_kafka_pyo3, ConfigError, ArrowKafkaError);
pyo3::create_exception!(arrow_kafka_pyo3, AdminError, ArrowKafkaError);

// ---------------------------------------------------------------------------
// SinkError → PyErr mapping
// ---------------------------------------------------------------------------

/// Convert a `SinkError` from the Rust core into the most specific Python
/// exception subclass.
///
/// `EnqueueError` receives a 2-tuple `(message, enqueued_so_far)` as its
/// `args` so callers can inspect `e.args[1]` (an `int`) to determine how
/// many rows were successfully enqueued before the failure.
fn sink_err_to_pyerr(e: SinkError) -> PyErr {
    let msg = e.to_string();
    match e {
        SinkError::SchemaRegistry { .. } => SchemaRegistryError::new_err(msg),
        SinkError::Serialization { .. } => SerializationError::new_err(msg),
        SinkError::Enqueue {
            enqueued_so_far, ..
        } => {
            // Pass (message, enqueued_so_far) so Python callers can do:
            //   except EnqueueError as exc:
            //       rows_done = exc.args[1]
            EnqueueError::new_err((msg, enqueued_so_far))
        }
        SinkError::FlushTimeout { .. } => FlushTimeoutError::new_err(msg),
        SinkError::UnsupportedType { .. } => UnsupportedTypeError::new_err(msg),
        SinkError::Config { .. } => ConfigError::new_err(msg),
        SinkError::Admin { .. } => AdminError::new_err(msg),
    }
}

// ---------------------------------------------------------------------------
// SinkStats Python class
// ---------------------------------------------------------------------------

/// A point-in-time snapshot of operational counters for an :class:`ArrowKafkaSink`.
///
/// All values are monotonically increasing from sink construction.
/// Take two snapshots to compute per-interval rates::
///
///     before = sink.stats()
///     time.sleep(1.0)
///     after = sink.stats()
///     rows_per_sec = after.enqueued_total - before.enqueued_total
///
/// Obtain via :meth:`ArrowKafkaSink.stats`.
#[pyclass(name = "SinkStats")]
pub struct SinkStatsPy {
    inner: SinkStats,
}

#[pymethods]
impl SinkStatsPy {
    /// Total rows successfully enqueued since the sink was created.
    ///
    /// Incremented for every row handed to the librdkafka send buffer by
    /// :meth:`ArrowKafkaSink.consume_arrow`.  Does **not** count broker
    /// acknowledgements — compare with :attr:`flush_count` to reason about
    /// delivery coverage.
    #[getter]
    pub fn enqueued_total(&self) -> u64 {
        self.inner.enqueued_total
    }

    /// Total :meth:`ArrowKafkaSink.flush` calls since construction, including
    /// those triggered internally by :meth:`ArrowKafkaSink.close`.
    ///
    /// A value of ``0`` after a long run is a strong signal that the caller is
    /// never waiting for broker acknowledgements.
    #[getter]
    pub fn flush_count(&self) -> u64 {
        self.inner.flush_count
    }

    /// Schema Registry lookups served from the in-process cache (no network).
    ///
    /// A cache hit occurs when :meth:`ArrowKafkaSink.consume_arrow` is called
    /// with a topic whose schema was already registered in a prior call.
    #[getter]
    pub fn sr_cache_hits(&self) -> u64 {
        self.inner.sr_cache_hits
    }

    /// Schema Registry lookups that required a network round-trip.
    ///
    /// Expected to be exactly 1 per unique ``(topic, schema)`` pair over the
    /// lifetime of the sink.  A persistently high miss rate indicates schema
    /// churn or a large number of distinct topics.
    #[getter]
    pub fn sr_cache_misses(&self) -> u64 {
        self.inner.sr_cache_misses
    }

    /// Total Schema Registry lookups (``sr_cache_hits + sr_cache_misses``).
    pub fn sr_total_lookups(&self) -> u64 {
        self.inner.sr_total_lookups()
    }

    /// Schema Registry cache hit-rate in ``[0.0, 1.0]``.
    ///
    /// Returns ``1.0`` when no lookups have been made yet (cold sink with
    /// zero traffic is considered a perfect cache, not a miss).
    pub fn sr_hit_rate(&self) -> f64 {
        self.inner.sr_hit_rate()
    }

    pub fn __repr__(&self) -> String {
        self.inner.to_string()
    }
}

// ---------------------------------------------------------------------------
// ArrowKafkaSink Python class
// ---------------------------------------------------------------------------

/// High-performance Kafka sink that produces ``pyarrow.Table`` data as
/// Confluent-framed Avro messages.
///
/// Each Kafka value payload has the Confluent wire format::
///
///     [0x00][schema_id: 4 bytes big-endian][avro_datum]
///
/// This format is consumed natively by Materialize::
///
///     CREATE SOURCE … VALUE FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY …
///
/// **Delivery model**: :meth:`consume_arrow` enqueues rows into the
/// librdkafka internal send buffer and returns as soon as all rows are
/// enqueued — not when they are broker-acknowledged.  Call :meth:`flush`
/// to block until full broker acknowledgement.
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
        max_in_flight        = 1000,
        linger_ms            = 20,
        batch_size           = 65536,
        compression_type     = "none".to_string(),
        subject_name_strategy = "topic_name".to_string(),
        enable_idempotence   = false,
        acks                 = "1".to_string(),
        retries              = None,
        retry_backoff_ms     = 100,
        request_timeout_ms   = 30000,
    ))]
    pub fn new(
        kafka_servers: String,
        schema_registry_url: String,
        max_in_flight: usize,
        linger_ms: u32,
        batch_size: usize,
        compression_type: String,
        subject_name_strategy: String,
        enable_idempotence: bool,
        acks: String,
        retries: Option<u32>,
        retry_backoff_ms: u32,
        request_timeout_ms: u32,
    ) -> PyResult<Self> {
        let strategy = SubjectNameStrategy::from_str(&subject_name_strategy)
            .map_err(|e| ConfigError::new_err(e))?;

        let inner = CoreSink::new(
            kafka_servers,
            schema_registry_url,
            max_in_flight,
            linger_ms,
            batch_size,
            compression_type,
            strategy,
            enable_idempotence,
            acks,
            retries,
            retry_backoff_ms,
            request_timeout_ms,
        )
        .map_err(sink_err_to_pyerr)?;

        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    /// Send a ``pyarrow.Table`` to a Kafka topic.
    ///
    /// Each row is serialised to Avro (Confluent wire format) and enqueued
    /// into the producer's internal send buffer.  Returns when all rows are
    /// **enqueued** — broker acknowledgement happens asynchronously.  Call
    /// :meth:`flush` afterwards to wait for full delivery confirmation.
    ///
    /// The GIL is released for the duration of serialisation and enqueuing.
    ///
    /// :param table: ``pyarrow.Table`` to produce.
    /// :param topic: Destination Kafka topic name.
    /// :param key_cols: Column names joined by ``key_separator`` to form the
    ///     Kafka message key.  Rows with any ``NULL`` key column produce a
    ///     keyless message.  ``None`` (default) → all messages keyless.
    /// :param key_separator: Separator between key column values.  Default ``"_"``.
    /// :param timeout_ms: Optional wall-clock deadline in milliseconds for the
    ///     enqueue operation.  When the producer queue is full and the deadline
    ///     elapses, raises :exc:`EnqueueError` with ``exc.args[1]`` = rows
    ///     already enqueued.  ``None`` (default) → retry indefinitely.
    ///
    /// :returns: Number of rows successfully enqueued (equals
    ///     ``table.num_rows`` on success).
    ///
    /// :raises SchemaRegistryError: Schema registration or lookup failed.
    /// :raises SerializationError: A row could not be serialised to Avro.
    /// :raises EnqueueError: librdkafka rejected the message or deadline
    ///     exceeded.  ``exc.args[1]`` = rows enqueued before failure.
    #[pyo3(signature = (
        table,
        topic,
        key_cols      = None,
        key_separator = "_".to_string(),
        timeout_ms    = None,
    ))]
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
        })
    }

    /// Flush all in-flight messages to the broker.
    ///
    /// Blocks until every message enqueued before this call has received a
    /// delivery report from the broker (success or permanent failure), or
    /// until ``timeout_ms`` elapses.
    ///
    /// **Delivery guarantee** — when this method returns without raising,
    /// all messages produced by preceding :meth:`consume_arrow` calls have
    /// been acknowledged by the broker (or a non-retryable delivery error was
    /// reported for them by librdkafka).
    ///
    /// The GIL is released for the duration of the blocking wait.
    ///
    /// :param timeout_ms: Max wait time in milliseconds.  Default: ``30000``.
    /// :raises FlushTimeoutError: Deadline reached before queue was drained.
    #[pyo3(signature = (timeout_ms = 30000))]
    pub fn flush(&self, py: Python<'_>, timeout_ms: u64) -> PyResult<()> {
        let sink = Arc::clone(&self.inner);
        py.detach(move || sink.flush(timeout_ms).map_err(sink_err_to_pyerr))
    }

    /// Flush all in-flight messages and release producer resources.
    ///
    /// Equivalent to calling :meth:`flush` with a 30-second timeout.
    /// The GIL is released for the duration of the blocking flush.
    ///
    /// :raises FlushTimeoutError: If the 30-second flush timeout is exceeded.
    pub fn close(&self, py: Python<'_>) -> PyResult<()> {
        let sink = Arc::clone(&self.inner);
        py.detach(move || sink.close().map_err(sink_err_to_pyerr))
    }

    /// Return a point-in-time snapshot of operational counters.
    ///
    /// All values are monotonically increasing since construction.
    /// Call this method twice and subtract to compute per-interval rates.
    ///
    /// :returns: :class:`SinkStats` snapshot.
    pub fn stats(&self) -> SinkStatsPy {
        SinkStatsPy {
            inner: self.inner.stats(),
        }
    }
}

// ---------------------------------------------------------------------------
// Module-level functions
// ---------------------------------------------------------------------------

/// Ensure a Kafka topic exists, creating it if necessary.
///
/// Returns ``True`` if the topic was **newly created**, ``False`` if it
/// **already existed**.
///
/// Any other broker error (insufficient permissions, replication factor
/// exceeds cluster size, etc.) raises :exc:`AdminError`.
///
/// The GIL is released for the duration of the blocking admin operation.
///
/// :param bootstrap_servers: Comma-separated ``host:port`` bootstrap servers.
/// :param topic: Name of the topic to create.
/// :param num_partitions: Number of partitions for the new topic.
///     Ignored if the topic already exists.  Default: ``1``.
/// :param replication_factor: Replication factor.  Must be ≤ the number of
///     brokers in the cluster.  Ignored if the topic already exists.
///     Default: ``1``.
/// :param timeout_ms: Max time to wait for a broker response in milliseconds.
///     Default: ``10000`` (10 seconds).
///
/// :returns: ``True`` = topic created, ``False`` = topic already existed.
/// :raises AdminError: Broker unreachable, rejected request, or timeout.
///
/// Example::
///
///     from arrow_kafka_pyo3 import create_topic_if_not_exists, AdminError
///
///     try:
///         created = create_topic_if_not_exists(
///             "localhost:9092", "my_events",
///             num_partitions=6, replication_factor=1,
///         )
///         print("created" if created else "already existed")
///     except AdminError as exc:
///         print(f"topic admin failed: {exc}")
#[pyfunction]
#[pyo3(signature = (
    bootstrap_servers,
    topic,
    num_partitions     = 1,
    replication_factor = 1,
    timeout_ms         = 10000,
))]
fn create_topic_if_not_exists(
    py: Python<'_>,
    bootstrap_servers: String,
    topic: String,
    num_partitions: i32,
    replication_factor: i32,
    timeout_ms: u64,
) -> PyResult<bool> {
    py.detach(move || {
        admin::create_topic_if_not_exists(
            &bootstrap_servers,
            &topic,
            num_partitions,
            replication_factor,
            timeout_ms,
        )
        .map_err(sink_err_to_pyerr)
    })
}

// ---------------------------------------------------------------------------
// Module registration
// ---------------------------------------------------------------------------

#[pymodule]
fn arrow_kafka_pyo3(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = m.py();

    // Classes.
    m.add_class::<ArrowKafkaSinkPy>()?;
    m.add_class::<SinkStatsPy>()?;

    // Module-level functions.
    m.add_function(wrap_pyfunction!(create_topic_if_not_exists, m)?)?;

    // Exception hierarchy — all exposed at module top level so callers can do
    // `from arrow_kafka_pyo3 import EnqueueError`.
    m.add("ArrowKafkaError", py.get_type::<ArrowKafkaError>())?;
    m.add("SchemaRegistryError", py.get_type::<SchemaRegistryError>())?;
    m.add("SerializationError", py.get_type::<SerializationError>())?;
    m.add("EnqueueError", py.get_type::<EnqueueError>())?;
    m.add("FlushTimeoutError", py.get_type::<FlushTimeoutError>())?;
    m.add(
        "UnsupportedTypeError",
        py.get_type::<UnsupportedTypeError>(),
    )?;
    m.add("ConfigError", py.get_type::<ConfigError>())?;
    m.add("AdminError", py.get_type::<AdminError>())?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Convert a `pyarrow.Table` into `Vec<RecordBatch>` via the Arrow C Stream
/// Interface (zero-copy where possible), then call `sink.consume_arrow`.
///
/// Briefly re-acquires the GIL via `Python::attach` for the PyArrow → Arrow
/// FFI conversion, then releases it again for the Rust serialisation work.
fn consume_arrow_impl(
    sink: &CoreSink,
    table: &Py<PyAny>,
    topic: &str,
    key_cols: &[String],
    key_separator: &str,
    timeout_ms: Option<u64>,
) -> PyResult<usize> {
    // Re-acquire GIL briefly for the FFI conversion from PyArrow.
    let batches: Vec<RecordBatch> = Python::attach(|py| -> PyResult<_> {
        let bound = table.bind(py);
        let reader = ArrowArrayStreamReader::from_pyarrow_bound(bound).map_err(|e| {
            PyRuntimeError::new_err(format!("PyArrow → Arrow stream conversion error: {e}"))
        })?;

        let mut out = Vec::new();
        for batch_result in reader {
            let batch = batch_result
                .map_err(|e| PyRuntimeError::new_err(format!("Arrow stream read error: {e}")))?;
            out.push(batch);
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

    // Pure Rust serialisation + librdkafka enqueuing — GIL not held.
    sink.consume_arrow(&batches, topic, key_cols_opt, key_separator, timeout_ms)
        .map_err(sink_err_to_pyerr)
}
