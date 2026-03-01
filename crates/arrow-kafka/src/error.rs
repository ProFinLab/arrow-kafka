use std::fmt;

/// Structured error type for all `ArrowKafkaSink` operations.
///
/// Every variant carries enough context (subject, topic, field name, enqueued row count)
/// for operational triage without requiring log-level correlation.
///
/// This type is the single error surface exposed by the `arrow-kafka` crate.
/// The PyO3 layer maps each variant to a dedicated Python exception subclass.
#[derive(Debug)]
pub enum SinkError {
    /// Schema Registry registration or lookup failed.
    ///
    /// Triggered when the sink cannot POST the inferred Avro schema to the registry,
    /// or when the registry returns an error response.
    SchemaRegistry {
        /// The full subject name that was attempted (e.g. `"my_topic-value"`).
        subject: String,
        /// The underlying error string from the registry client.
        cause: String,
    },

    /// Row-level Avro serialization failed.
    ///
    /// Triggered by type mismatches, null values in non-nullable fields, or
    /// Arrow types that are not yet supported by the converter.
    Serialization {
        /// Kafka topic the batch was being sent to.
        topic: String,
        /// Field descriptor that identifies *where* the failure occurred.
        /// Format: `"row[N]"` when the failing field is determined at row level,
        /// or `"<schema>"` when the converter itself could not be initialised.
        field: String,
        /// The underlying error string.
        cause: String,
    },

    /// Producer enqueue failed — either a fatal librdkafka error or a deadline was
    /// exceeded while the internal producer queue was full.
    ///
    /// Rows reported by `enqueued_so_far` have been handed to the producer and
    /// *may* still be delivered asynchronously even after this error is returned.
    /// Call `flush()` to drain them if you need certainty.
    Enqueue {
        /// Kafka topic the batch was being sent to.
        topic: String,
        /// Number of rows successfully enqueued **before** the failure.
        /// Exposed as `e.args[1]` on the Python `EnqueueError`.
        enqueued_so_far: usize,
        /// The underlying error string.
        cause: String,
    },

    /// `flush()` deadline exceeded before all in-flight messages were acknowledged
    /// by the broker.
    ///
    /// After this error the producer may still have messages in its queue.
    /// Retry `flush()` with a longer timeout, or call `close()` to give up.
    FlushTimeout {
        /// The timeout value (milliseconds) that was exceeded.
        timeout_ms: u64,
    },

    /// An Arrow column has a data type that is not yet supported by the Avro converter.
    ///
    /// Upgrade the `arrow-kafka` crate or cast the column to a supported type
    /// before producing.
    UnsupportedType {
        /// Name of the Arrow field whose type is unsupported.
        field: String,
        /// Human-readable Arrow data-type string (e.g. `"Date32"`, `"Timestamp(Microsecond, None)"`).
        arrow_type: String,
    },

    /// Invalid sink configuration supplied at construction time.
    ///
    /// Examples: incompatible `max_in_flight` + idempotence settings,
    /// librdkafka rejecting a config key/value pair.
    Config {
        /// Description of the configuration problem.
        cause: String,
    },

    /// Kafka topic administration operation failed.
    ///
    /// Raised by `create_topic_if_not_exists` when the AdminClient cannot
    /// reach the broker or the broker rejects the request for any reason
    /// other than `TopicAlreadyExists`.
    Admin {
        /// The topic name that was being operated on.
        topic: String,
        /// The underlying error string from librdkafka or the runtime.
        cause: String,
    },
}

// ---------------------------------------------------------------------------
// Display — messages are the canonical text surfaced to Python callers.
// Keep them machine-parseable: `[key=value]` tags come before the free-form cause.
// ---------------------------------------------------------------------------

impl fmt::Display for SinkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SchemaRegistry { subject, cause } => {
                write!(f, "schema registry error [subject={subject}]: {cause}")
            }

            Self::Serialization {
                topic,
                field,
                cause,
            } => {
                write!(
                    f,
                    "serialization error [topic={topic}, field={field}]: {cause}"
                )
            }

            Self::Enqueue {
                topic,
                enqueued_so_far,
                cause,
            } => {
                write!(
                    f,
                    "enqueue error [topic={topic}, enqueued_so_far={enqueued_so_far}]: {cause}"
                )
            }

            Self::FlushTimeout { timeout_ms } => {
                write!(
                    f,
                    "flush timeout [timeout_ms={timeout_ms}]: \
                     deadline exceeded before all in-flight messages were acknowledged"
                )
            }

            Self::UnsupportedType { field, arrow_type } => {
                write!(
                    f,
                    "unsupported Arrow type [field={field}, type={arrow_type}]: \
                     cast the column to a supported type before producing"
                )
            }

            Self::Config { cause } => {
                write!(f, "configuration error: {cause}")
            }

            Self::Admin { topic, cause } => {
                write!(f, "admin error [topic={topic}]: {cause}")
            }
        }
    }
}

impl std::error::Error for SinkError {}
