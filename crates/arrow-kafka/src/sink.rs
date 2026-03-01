use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use arrow::record_batch::RecordBatch;
use rdkafka::ClientConfig;
use rdkafka::error::KafkaError;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::types::RDKafkaErrorCode;

use crate::converter::ArrowToAvroConverter;
use crate::error::SinkError;
use crate::key::build_keys_for_batch;
use crate::schema_registry::{SrClient, SubjectNameStrategy};
use crate::stats::SinkStats;

// ---------------------------------------------------------------------------
// ArrowKafkaSink
// ---------------------------------------------------------------------------

/// High-performance Kafka sink that serialises `RecordBatch` slices to
/// Confluent-framed Avro and produces them via `librdkafka`.
///
/// # Message format
/// Each Kafka value payload is encoded as:
/// ```text
/// [0x00][schema_id: u32 big-endian][avro_datum]
/// ```
/// This is the Confluent wire format, compatible with:
/// - Confluent Schema Registry consumers
/// - Redpanda Schema Registry
/// - Materialize `SOURCE … VALUE FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY`
///
/// # Delivery model
/// `consume_arrow` enqueues rows into librdkafka's internal send buffer and
/// returns as soon as all rows for the call are enqueued (not broker-acked).
/// Call `flush()` to block until every previously enqueued message has a
/// delivery report from the broker.
///
/// # Thread safety
/// The underlying `FutureProducer` is `Send + Sync`.  `ArrowKafkaSink` can be
/// wrapped in `Arc` and shared across threads.
pub struct ArrowKafkaSink {
    sr: SrClient,
    producer: FutureProducer,
    // ---- Stats counters (monotonically increasing, never reset) ----------
    /// Total rows successfully enqueued since construction.
    enqueued_total: Arc<AtomicU64>,
    /// Total `flush()` calls since construction (includes those from `close()`).
    flush_count: Arc<AtomicU64>,
}

impl ArrowKafkaSink {
    /// Construct a new sink.
    ///
    /// # Core parameters
    /// - `kafka_servers`           — Comma-separated `host:port` bootstrap servers.
    /// - `schema_registry_url`     — Base URL of the Confluent-compatible Schema Registry.
    /// - `max_in_flight`           — Maximum in-flight requests per broker connection
    ///   (`max.in.flight.requests.per.connection`).  Must be ≥ 1.
    ///   When `enable_idempotence` is `true`, must be ≤ 5 (librdkafka requirement).
    /// - `linger_ms`               — Producer batching linger time (`linger.ms`).
    /// - `batch_size`              — Max bytes in a single produce-request batch (`batch.size`).
    /// - `compression_type`        — One of `"none"`, `"gzip"`, `"snappy"`, `"lz4"`, `"zstd"`.
    /// - `subject_name_strategy`   — Subject naming strategy for the Schema Registry.
    ///
    /// # Reliability parameters
    /// - `enable_idempotence`  — Enable idempotent producer (`enable.idempotence`).
    ///   When `true`, librdkafka enforces exactly-once delivery per partition.
    ///   Requires `max_in_flight ≤ 5` and implicitly sets `acks = "all"`.
    /// - `acks`                — Broker acknowledgement level (`acks`).
    ///   `"0"` = fire-and-forget, `"1"` = leader ack (default), `"all"` = full ISR ack.
    /// - `retries`             — Number of times librdkafka retries a failed produce request
    ///   (`retries`).  `None` uses the librdkafka default (effectively unlimited).
    /// - `retry_backoff_ms`    — Time between retries (`retry.backoff.ms`).  Default 100 ms.
    /// - `request_timeout_ms`  — Per-request broker timeout (`request.timeout.ms`).
    ///   Default 30 000 ms.
    ///
    /// # Errors
    /// Returns `SinkError::Config` if any argument is invalid or if librdkafka
    /// rejects the producer configuration.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        kafka_servers: String,
        schema_registry_url: String,
        max_in_flight: usize,
        linger_ms: u32,
        batch_size: usize,
        compression_type: String,
        subject_name_strategy: SubjectNameStrategy,
        enable_idempotence: bool,
        acks: String,
        retries: Option<u32>,
        retry_backoff_ms: u32,
        request_timeout_ms: u32,
    ) -> Result<Self, SinkError> {
        // --- Validate ---------------------------------------------------
        if max_in_flight == 0 {
            return Err(SinkError::Config {
                cause: "max_in_flight must be >= 1".to_string(),
            });
        }
        if enable_idempotence && max_in_flight > 5 {
            return Err(SinkError::Config {
                cause: format!(
                    "enable_idempotence=true requires max_in_flight <= 5, \
                     got max_in_flight={max_in_flight}"
                ),
            });
        }

        // --- Schema Registry client -------------------------------------
        let sr = SrClient::new(schema_registry_url, subject_name_strategy).map_err(|e| {
            SinkError::Config {
                cause: format!("schema registry client init failed: {e}"),
            }
        })?;

        // --- librdkafka producer ----------------------------------------
        let mut cfg = ClientConfig::new();
        cfg.set("bootstrap.servers", &kafka_servers)
            .set("linger.ms", linger_ms.to_string())
            .set("batch.size", batch_size.to_string())
            .set("compression.type", &compression_type)
            // Task 1.1: max_in_flight now wired to librdkafka config.
            .set(
                "max.in.flight.requests.per.connection",
                max_in_flight.to_string(),
            )
            // Allow a large internal queue so that send_result rarely returns
            // QueueFull in normal operation.  The actual backpressure point is
            // the broker, reached via flush().
            .set("queue.buffering.max.messages", "1000000")
            // --- Task 3.1 reliability knobs --------------------------------
            .set("enable.idempotence", enable_idempotence.to_string())
            .set("acks", &acks)
            .set("retry.backoff.ms", retry_backoff_ms.to_string())
            .set("request.timeout.ms", request_timeout_ms.to_string());

        // retries is optional; when None we let librdkafka use its default
        // (effectively unlimited when idempotence is off, or enforced when on).
        if let Some(r) = retries {
            cfg.set("retries", r.to_string());
        }

        let producer: FutureProducer = cfg.create().map_err(|e| SinkError::Config {
            cause: format!("librdkafka producer creation failed: {e}"),
        })?;

        Ok(Self {
            sr,
            producer,
            enqueued_total: Arc::new(AtomicU64::new(0)),
            flush_count: Arc::new(AtomicU64::new(0)),
        })
    }

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /// Serialise a slice of `RecordBatch`es to Avro and enqueue them for
    /// delivery to `topic`.
    ///
    /// # Return value
    /// Returns the number of rows successfully **enqueued** into the
    /// librdkafka send buffer.  On success this always equals the total
    /// number of rows across all batches.
    ///
    /// Enqueued ≠ broker-acknowledged.  Call [`flush`](Self::flush) afterwards
    /// to block until broker acknowledgement.
    ///
    /// # Arguments
    /// - `key_cols`           — Column names whose string representations are
    ///   concatenated (with `key_separator`) to form the Kafka message key.
    ///   Rows where any key column is NULL produce a keyless message.
    ///   Pass `None` or an empty slice for keyless messages.
    /// - `enqueue_timeout_ms` — Optional wall-clock deadline for the entire
    ///   enqueue operation.  If the producer queue is full and the deadline
    ///   elapses before a row can be enqueued, the call returns
    ///   `SinkError::Enqueue { enqueued_so_far, … }` so the caller can
    ///   decide whether to retry or abort.  When `None`, the call will
    ///   retry indefinitely until every row is enqueued.
    ///
    /// # Errors
    /// - `SinkError::SchemaRegistry` — schema registration failed.
    /// - `SinkError::Serialization`  — a row could not be serialised to Avro.
    /// - `SinkError::Enqueue`        — librdkafka rejected the message
    ///   (fatal error or deadline exceeded while queue was full).
    pub fn consume_arrow(
        &self,
        batches: &[RecordBatch],
        topic: &str,
        key_cols: Option<&[String]>,
        key_separator: &str,
        enqueue_timeout_ms: Option<u64>,
    ) -> Result<usize, SinkError> {
        if batches.is_empty() {
            return Ok(0);
        }

        let schema = batches[0].schema();
        let converter =
            ArrowToAvroConverter::new(schema).map_err(|e| SinkError::Serialization {
                topic: topic.to_string(),
                field: "<schema>".to_string(),
                cause: e.to_string(),
            })?;

        // Convert the optional millisecond budget into an absolute Instant
        // deadline that can be cheaply checked inside the hot loop.
        let deadline = enqueue_timeout_ms.map(|ms| Instant::now() + Duration::from_millis(ms));

        self.process_and_send(
            batches,
            topic,
            &converter,
            key_cols,
            key_separator,
            deadline,
        )
    }

    /// Flush all in-flight messages to the broker.
    ///
    /// Blocks until every message enqueued before this call has received a
    /// delivery report (success or permanent failure) from the broker, or
    /// until `timeout_ms` elapses.
    ///
    /// # Delivery guarantee
    /// When this method returns `Ok(())`, **all messages enqueued by prior
    /// `consume_arrow` calls have been acknowledged by the broker** (or a
    /// permanent, non-retryable delivery error has been reported for them by
    /// librdkafka).  This makes it safe to call `flush()` at task-shutdown
    /// boundaries to assert end-to-end delivery.
    ///
    /// # Errors
    /// Returns `SinkError::FlushTimeout { timeout_ms }` if the deadline is
    /// reached before the queue is fully drained.  In that case some messages
    /// may still be in the producer queue; retry with a longer timeout or
    /// accept the data loss.
    pub fn flush(&self, timeout_ms: u64) -> Result<(), SinkError> {
        let result = self
            .producer
            .flush(Duration::from_millis(timeout_ms))
            .map_err(|_| SinkError::FlushTimeout { timeout_ms });

        // Increment flush_count even on error so callers can detect attempts
        // vs. successes by comparing against their own success tracking.
        self.flush_count.fetch_add(1, Ordering::Relaxed);

        result
    }

    /// Flush all in-flight messages and release producer resources.
    ///
    /// Calls `flush(30_000)` internally.  Any error from flush is propagated.
    ///
    /// # Errors
    /// Returns `SinkError::FlushTimeout` if the 30-second timeout elapses
    /// before the queue is drained.
    pub fn close(&self) -> Result<(), SinkError> {
        self.flush(30_000)
    }

    /// Return a point-in-time snapshot of all operational counters.
    ///
    /// All returned values are monotonically increasing since construction.
    /// Take two snapshots to compute per-interval rates.
    ///
    /// See [`SinkStats`] for field documentation and alerting guidance.
    pub fn stats(&self) -> SinkStats {
        let (sr_hits, sr_misses) = self.sr.cache_stats();
        SinkStats::new(
            self.enqueued_total.load(Ordering::Relaxed),
            self.flush_count.load(Ordering::Relaxed),
            sr_hits,
            sr_misses,
        )
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// Inner loop: register schema, build keys, serialise rows, enqueue.
    fn process_and_send(
        &self,
        batches: &[RecordBatch],
        topic: &str,
        converter: &ArrowToAvroConverter,
        key_cols: Option<&[String]>,
        key_separator: &str,
        deadline: Option<Instant>,
    ) -> Result<usize, SinkError> {
        let mut enqueued = 0usize;

        // -- Schema registration ---------------------------------------------
        // `get_or_register_schema_id` is cached in memory after the first
        // call for a given (topic, schema) pair; subsequent calls are O(1)
        // hash-map lookups with no network I/O.
        let schema_id = self
            .sr
            .get_or_register_schema_id(topic, converter.schema_json(), converter.schema_name())
            .map_err(|e| SinkError::SchemaRegistry {
                subject: self.sr.subject(topic, converter.schema_name()),
                cause: e.to_string(),
            })?;

        for batch in batches {
            // -- Key construction --------------------------------------------
            let keys = build_keys_for_batch(batch, key_cols.unwrap_or(&[]), key_separator)
                .map_err(|e| SinkError::Serialization {
                    topic: topic.to_string(),
                    field: "<key>".to_string(),
                    cause: e.to_string(),
                })?;

            // -- Row-level serialisation + enqueue ---------------------------
            for row_index in 0..batch.num_rows() {
                // Serialise the row to an Avro datum and wrap it in the
                // Confluent wire format (magic byte + schema_id + datum).
                let payload = converter.convert_row(batch, row_index).map_err(|e| {
                    SinkError::Serialization {
                        topic: topic.to_string(),
                        // row[N] identifies the failing row; the full cause
                        // string from the converter also contains the field
                        // name and Arrow type.
                        field: format!("row[{row_index}]"),
                        cause: e.to_string(),
                    }
                })?;

                let framed = SrClient::confluent_frame(schema_id, &payload);

                let key_bytes_opt = keys
                    .get(row_index)
                    .and_then(|opt| opt.as_ref().map(|v| v.as_slice()));

                let mut record: FutureRecord<'_, [u8], [u8]> =
                    FutureRecord::to(topic).payload(&framed);

                if let Some(key_bytes) = key_bytes_opt {
                    record = record.key(key_bytes);
                }

                // -- Enqueue with deadline-bounded back-pressure -------------
                // Task 1.2: the queue-full retry loop now checks the caller's
                // deadline before each attempt.
                let mut record_to_send = record;
                loop {
                    // Check deadline *before* each retry so that a caller
                    // with a very tight budget can escape quickly.
                    if let Some(dl) = deadline {
                        if Instant::now() >= dl {
                            return Err(SinkError::Enqueue {
                                topic: topic.to_string(),
                                enqueued_so_far: enqueued,
                                cause: "enqueue deadline exceeded: \
                                        producer queue was still full at deadline"
                                    .to_string(),
                            });
                        }
                    }

                    match self.producer.send_result(record_to_send) {
                        Ok(_) => {
                            enqueued += 1;
                            self.enqueued_total.fetch_add(1, Ordering::Relaxed);
                            break;
                        }
                        // Queue full: wait a short time and retry.  This is
                        // the normal back-pressure path when the broker is
                        // slower than the producer.
                        Err((
                            KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull),
                            returned,
                        )) => {
                            std::thread::sleep(Duration::from_millis(5));
                            record_to_send = returned;
                        }
                        // Any other error is fatal for this batch.
                        Err((e, _)) => {
                            return Err(SinkError::Enqueue {
                                topic: topic.to_string(),
                                enqueued_so_far: enqueued,
                                cause: format!("librdkafka fatal error: {e}"),
                            });
                        }
                    }
                }
            }
        }

        Ok(enqueued)
    }
}
