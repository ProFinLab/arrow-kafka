use std::fmt;

// ---------------------------------------------------------------------------
// SinkStats
// ---------------------------------------------------------------------------

/// A point-in-time snapshot of operational counters for an [`ArrowKafkaSink`].
///
/// All counters are **monotonically increasing** from the moment the sink was
/// constructed.  They never reset on flush or close.  Use two successive
/// snapshots to compute rates:
///
/// ```text
/// let before = sink.stats();
/// // ... produce for 1 second ...
/// let after  = sink.stats();
/// let rows_per_sec = after.enqueued_total - before.enqueued_total;
/// ```
///
/// Obtain via [`ArrowKafkaSink::stats()`](crate::sink::ArrowKafkaSink::stats).
///
/// # Observability use-cases
///
/// | Counter            | Alert on                                   |
/// |--------------------|--------------------------------------------|
/// | `enqueued_total`   | stalled growth → producer is blocked       |
/// | `flush_count`      | unexpectedly low → flush not being called  |
/// | `sr_cache_misses`  | high rate → schema thrash / many topics    |
/// | `sr_cache_hits`    | very low → cache not working               |
///
/// [`ArrowKafkaSink`]: crate::sink::ArrowKafkaSink
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinkStats {
    /// Total rows successfully enqueued into the librdkafka send buffer
    /// since this sink was constructed.
    ///
    /// Incremented by `consume_arrow` for every row handed to the producer.
    /// Does **not** count broker acknowledgements — use in combination with
    /// `flush_count` to reason about delivery coverage.
    pub enqueued_total: u64,

    /// Total number of `flush()` calls since construction, including those
    /// triggered internally by `close()`.
    ///
    /// A value of 0 after a long run is a signal that the caller is never
    /// flushing, meaning broker acknowledgements are not being waited for.
    pub flush_count: u64,

    /// Number of Schema Registry lookups served from the in-process cache
    /// without a network round-trip.
    ///
    /// A cache hit occurs when `consume_arrow` is called with a topic whose
    /// schema has already been registered in a prior call.
    pub sr_cache_hits: u64,

    /// Number of Schema Registry lookups that required a network round-trip
    /// to register or look up the schema.
    ///
    /// Expected to be 1 per unique `(topic, schema)` pair over the lifetime
    /// of the sink.  A consistently high miss rate may indicate that the
    /// schema changes between calls or that many distinct topics are being
    /// used.
    pub sr_cache_misses: u64,
}

impl SinkStats {
    /// Construct a stats snapshot from raw counter values.
    ///
    /// Callers are `ArrowKafkaSink::stats()` which reads the atomics and
    /// passes the loaded `u64` values here.
    pub(crate) fn new(
        enqueued_total: u64,
        flush_count: u64,
        sr_cache_hits: u64,
        sr_cache_misses: u64,
    ) -> Self {
        Self {
            enqueued_total,
            flush_count,
            sr_cache_hits,
            sr_cache_misses,
        }
    }

    /// Total Schema Registry lookups (hits + misses).
    ///
    /// Useful for computing the cache hit-rate percentage:
    ///
    /// ```text
    /// let hit_rate = stats.sr_cache_hits as f64 / stats.sr_total_lookups() as f64;
    /// ```
    pub fn sr_total_lookups(&self) -> u64 {
        self.sr_cache_hits.saturating_add(self.sr_cache_misses)
    }

    /// Schema Registry cache hit-rate as a value in `[0.0, 1.0]`.
    ///
    /// Returns `1.0` (perfect cache) when no lookups have been made yet,
    /// matching the intuition that a cold sink with zero traffic is not
    /// "missing" anything.
    pub fn sr_hit_rate(&self) -> f64 {
        let total = self.sr_total_lookups();
        if total == 0 {
            1.0
        } else {
            self.sr_cache_hits as f64 / total as f64
        }
    }
}

impl fmt::Display for SinkStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "SinkStats {{ enqueued_total={}, flush_count={}, \
             sr_cache_hits={}, sr_cache_misses={}, sr_hit_rate={:.2}% }}",
            self.enqueued_total,
            self.flush_count,
            self.sr_cache_hits,
            self.sr_cache_misses,
            self.sr_hit_rate() * 100.0,
        )
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make(hits: u64, misses: u64) -> SinkStats {
        SinkStats::new(0, 0, hits, misses)
    }

    #[test]
    fn sr_hit_rate_no_lookups_returns_one() {
        assert_eq!(make(0, 0).sr_hit_rate(), 1.0);
    }

    #[test]
    fn sr_hit_rate_all_hits() {
        assert_eq!(make(10, 0).sr_hit_rate(), 1.0);
    }

    #[test]
    fn sr_hit_rate_all_misses() {
        assert_eq!(make(0, 10).sr_hit_rate(), 0.0);
    }

    #[test]
    fn sr_hit_rate_mixed() {
        let r = make(3, 1).sr_hit_rate();
        assert!((r - 0.75).abs() < 1e-9, "expected 0.75, got {r}");
    }

    #[test]
    fn sr_total_lookups_sums_correctly() {
        assert_eq!(make(7, 3).sr_total_lookups(), 10);
    }

    #[test]
    fn display_contains_all_fields() {
        let s = SinkStats::new(1000, 5, 99, 1);
        let text = s.to_string();
        assert!(text.contains("enqueued_total=1000"), "{text}");
        assert!(text.contains("flush_count=5"), "{text}");
        assert!(text.contains("sr_cache_hits=99"), "{text}");
        assert!(text.contains("sr_cache_misses=1"), "{text}");
        assert!(text.contains("sr_hit_rate="), "{text}");
    }

    #[test]
    fn clone_produces_equal_value() {
        let s = SinkStats::new(42, 3, 10, 2);
        assert_eq!(s.clone(), s);
    }
}
