//! Kafka topic administration helpers.
//!
//! These are **standalone functions** (not methods on [`ArrowKafkaSink`]) so
//! they can be called before a sink is constructed — for example to ensure a
//! topic exists before the first `consume_arrow` call.
//!
//! Each function creates its own short-lived librdkafka `AdminClient` and a
//! single-threaded Tokio runtime to drive the async admin operation, then
//! tears both down before returning.  The overhead is intentional: admin
//! operations are rare (typically once at startup), and this design avoids
//! leaking a persistent Tokio runtime into the Python embedding context.
//!
//! [`ArrowKafkaSink`]: crate::sink::ArrowKafkaSink

use std::time::Duration;

use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::error::RDKafkaErrorCode;

use crate::error::SinkError;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Ensure a Kafka topic exists, creating it if necessary.
///
/// # Behaviour
/// * If the topic **does not exist**, it is created with the given partition
///   and replication settings and the function returns `Ok(true)`.
/// * If the topic **already exists**, the function returns `Ok(false)` without
///   modifying the existing topic configuration.
/// * Any other error (broker unreachable, insufficient permissions, etc.)
///   is returned as `Err(SinkError::Admin { .. })`.
///
/// # Arguments
/// - `bootstrap_servers`  — Comma-separated `host:port` bootstrap servers.
/// - `topic`              — Name of the topic to create.
/// - `num_partitions`     — Number of partitions for the new topic.
///   Ignored if the topic already exists.
/// - `replication_factor` — Replication factor.  Must be ≤ the number of
///   brokers in the cluster.  Ignored if the topic already exists.
/// - `timeout_ms`         — Maximum time to wait for the broker to respond,
///   in milliseconds.  The function will return an error if the deadline is
///   exceeded.
///
/// # Errors
/// Returns [`SinkError::Admin`] when:
/// * The Tokio runtime cannot be built.
/// * The `AdminClient` cannot be created (bad bootstrap address format, etc.).
/// * The broker returns an error other than `TopicAlreadyExists`.
/// * The broker does not respond within `timeout_ms`.
///
/// # Example
/// ```no_run
/// use arrow_kafka::admin::create_topic_if_not_exists;
///
/// let created = create_topic_if_not_exists(
///     "localhost:9092",
///     "my_events",
///     /*num_partitions=*/ 6,
///     /*replication_factor=*/ 1,
///     /*timeout_ms=*/ 10_000,
/// ).unwrap();
///
/// println!("{}", if created { "topic created" } else { "topic already existed" });
/// ```
pub fn create_topic_if_not_exists(
    bootstrap_servers: &str,
    topic: &str,
    num_partitions: i32,
    replication_factor: i32,
    timeout_ms: u64,
) -> Result<bool, SinkError> {
    // Build a minimal single-threaded Tokio runtime.  We don't need the full
    // multi-thread scheduler or any IO/time drivers — rdkafka drives its own
    // poll loop internally.
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .map_err(|e| SinkError::Admin {
            topic: topic.to_string(),
            cause: format!("failed to build Tokio runtime for admin operation: {e}"),
        })?;

    // Create a short-lived AdminClient.
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        // Cap metadata/connection timeout so we don't hang indefinitely
        // if the broker is unreachable.
        .set("socket.timeout.ms", timeout_ms.to_string())
        .set("metadata.request.timeout.ms", timeout_ms.to_string())
        .create()
        .map_err(|e| SinkError::Admin {
            topic: topic.to_string(),
            cause: format!("failed to create AdminClient: {e}"),
        })?;

    // Build the topic descriptor.
    let new_topic = NewTopic::new(
        topic,
        num_partitions,
        TopicReplication::Fixed(replication_factor),
    );

    // Set the per-request timeout that the broker will enforce.
    let opts = AdminOptions::new().request_timeout(Some(Duration::from_millis(timeout_ms)));

    // Drive the async create_topics future on our single-shot runtime.
    let results = rt
        .block_on(admin.create_topics(&[new_topic], &opts))
        .map_err(|e| SinkError::Admin {
            topic: topic.to_string(),
            cause: format!("AdminClient::create_topics failed: {e}"),
        })?;

    // `results` is Vec<TopicResult> where TopicResult = Result<String, (String, RDKafkaErrorCode)>.
    // There is exactly one entry since we submitted exactly one topic.
    for result in results {
        match result {
            Ok(created_name) => {
                debug_assert_eq!(
                    created_name, topic,
                    "broker returned a different topic name than requested"
                );
                return Ok(true); // newly created
            }
            Err((name, RDKafkaErrorCode::TopicAlreadyExists)) => {
                debug_assert_eq!(name, topic);
                return Ok(false); // already existed — not an error
            }
            Err((name, code)) => {
                return Err(SinkError::Admin {
                    topic: name,
                    cause: format!("broker returned error: {code:?}"),
                });
            }
        }
    }

    // Unreachable in practice: the broker always returns exactly one result
    // per requested topic.  Treat an empty result as an unexpected error.
    Err(SinkError::Admin {
        topic: topic.to_string(),
        cause: "broker returned an empty result set for create_topics request".to_string(),
    })
}

// ---------------------------------------------------------------------------
// Tests (offline — no real broker required)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Confirm that a bad bootstrap address surfaces a descriptive `Admin` error
    /// rather than hanging or panicking.  We use a port that should never have
    /// a Kafka broker listening (high ephemeral range) and a very short timeout.
    #[test]
    fn unreachable_broker_returns_admin_error() {
        // A 500 ms timeout keeps the test fast even on slow CI runners.
        let result = create_topic_if_not_exists("127.0.0.1:19997", "will_never_exist", 1, 1, 500);

        match result {
            Err(SinkError::Admin { topic, cause }) => {
                assert_eq!(topic, "will_never_exist", "topic name echoed correctly");
                // The cause must be non-empty so operators have something to act on.
                assert!(
                    !cause.is_empty(),
                    "Admin error cause must be non-empty, got empty string"
                );
            }
            Ok(v) => panic!("expected Admin error for unreachable broker, got Ok({v})"),
            Err(e) => panic!("expected SinkError::Admin, got a different error variant: {e}"),
        }
    }

    #[test]
    fn topic_name_is_preserved_in_error() {
        let result = create_topic_if_not_exists("127.0.0.1:19998", "my_specific_topic", 3, 1, 500);
        match result {
            Err(SinkError::Admin { topic, .. }) => {
                assert_eq!(
                    topic, "my_specific_topic",
                    "topic name must be echoed verbatim in Admin error"
                );
            }
            Err(e) => panic!("expected SinkError::Admin, got a different error variant: {e}"),
            Ok(_) => { /* broker happened to be reachable in this environment — acceptable */ }
        }
        // If for some reason Ok is returned (e.g. a broker is running on
        // 19998 in this environment), that is also acceptable.
    }
}
