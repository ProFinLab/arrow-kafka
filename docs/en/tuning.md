# Performance Tuning Guide

This guide explains how to configure `arrow-kafka-pyo3` for optimal performance in different scenarios.

## Key Configuration Parameters

### Producer Tuning

| Parameter | librdkafka Equivalent | Description | Default |
|-----------|----------------------|-------------|---------|
| `linger_ms` | `linger.ms` | Time to wait for additional messages before sending a batch (milliseconds). Higher values improve throughput but increase latency. | `20` |
| `batch_size` | `batch.size` | Maximum size of a single produce request batch (bytes). Larger batches improve throughput but increase memory usage. | `65536` |
| `compression_type` | `compression.type` | Compression codec: `"none"`, `"gzip"`, `"snappy"`, `"lz4"`, `"zstd"`. Compression reduces network bandwidth but increases CPU usage. | `"none"` |
| `max_in_flight` | `max.in.flight.requests.per.connection` | Maximum number of unacknowledged requests per broker connection. Must be ≤5 when `enable_idempotence=True`. | `1000` |

### Reliability Tuning

| Parameter | librdkafka Equivalent | Description | Default |
|-----------|----------------------|-------------|---------|
| `enable_idempotence` | `enable.idempotence` | Enable exactly-once delivery semantics per partition. Requires `max_in_flight≤5`. | `False` |
| `acks` | `acks` | Broker acknowledgement level: `"0"` (fire-and-forget), `"1"` (leader ack), `"all"` (full ISR ack). | `"1"` |
| `retries` | `retries` | Number of times to retry failed produce requests. `None` uses librdkafka default. | `None` |
| `retry_backoff_ms` | `retry.backoff.ms` | Time between retries (milliseconds). | `100` |
| `request_timeout_ms` | `request.timeout.ms` | Per-request broker timeout (milliseconds). | `30000` |

### Schema Registry Tuning

| Parameter | Description | Default |
|-----------|-------------|---------|
| `subject_name_strategy` | Controls how Schema Registry subject names are derived. Options: `"topic_name"` (`{topic}-value`), `"record_name"`, `"topic_record_name"`. | `"topic_name"` |

## Preset Configuration Profiles

### Profile 1: Low Latency

Use this profile when minimizing end-to-end latency is critical (e.g., real-time trading systems, interactive applications).

```python
sink = ArrowKafkaSink(
    kafka_servers="localhost:9092",
    schema_registry_url="http://localhost:8081",
    # --- Producer Tuning ---
    linger_ms=0,           # Send immediately, no batching delay
    batch_size=16384,      # Small batches for low latency
    compression_type="none", # Skip compression for CPU savings
    max_in_flight=5,       # Small in-flight window
    # --- Reliability ---
    enable_idempotence=False,  # Disable for lower overhead
    acks="1",              # Leader ack balances latency & reliability
    retries=3,             # Limited retries to avoid tail latency
    retry_backoff_ms=50,   # Short backoff for quick retries
    # --- Timeouts ---
    request_timeout_ms=5000,  # Aggressive timeout
)
```

**Characteristics:**
- **Latency**: 1-10ms typical
- **Throughput**: 10-100 MB/s
- **Use case**: Real-time processing, interactive applications
- **Trade-off**: Higher CPU usage per message, lower throughput

### Profile 2: High Throughput

Use this profile when maximizing throughput is critical (e.g., data ingestion, ETL pipelines, log aggregation).

```python
sink = ArrowKafkaSink(
    kafka_servers="localhost:9092",
    schema_registry_url="http://localhost:8081",
    # --- Producer Tuning ---
    linger_ms=20,          # Batch for up to 20ms
    batch_size=1048576,    # 1MB batches
    compression_type="lz4", # Good balance of speed and ratio
    max_in_flight=1000,    # Large in-flight window
    # --- Reliability ---
    enable_idempotence=True, # Exactly-once delivery
    acks="all",           # Wait for full ISR ack
    retries=None,         # Use librdkafka default (effectively unlimited)
    retry_backoff_ms=100,  # Standard backoff
    # --- Timeouts ---
    request_timeout_ms=30000,  # Conservative timeout
)
```

**Characteristics:**
- **Latency**: 20-100ms typical
- **Throughput**: 500 MB/s - 1 GB/s+
- **Use case**: Batch processing, data warehousing, log aggregation
- **Trade-off**: Higher memory usage, increased tail latency

## Tuning Recommendations

### 1. Memory vs. Latency Trade-off

- **Small `batch_size` + low `linger_ms`** = Low latency, higher CPU overhead
- **Large `batch_size` + high `linger_ms`** = High throughput, higher memory usage

**Rule of thumb:** Set `batch_size` to match your typical message size multiplied by expected batch size.

### 2. Compression Selection

| Codec | Ratio | Speed | CPU | Best For |
|-------|-------|-------|-----|----------|
| `none` | 1.0x | Fastest | Lowest | Latency-sensitive workloads |
| `snappy` | 2.0-2.5x | Fast | Low | General purpose |
| `lz4` | 2.5-3.0x | Very Fast | Medium | High-throughput streaming |
| `zstd` | 3.0-5.0x | Medium | High | Bandwidth-constrained networks |
| `gzip` | 3.0-5.0x | Slow | High | Archival, batch processing |

### 3. Reliability vs. Performance

- **`acks="0"`**: Maximum throughput, no delivery guarantees
- **`acks="1"`**: Good balance (default)
- **`acks="all"`**: Maximum reliability, lower throughput

**Recommendation:** Use `enable_idempotence=True` with `acks="all"` for critical financial data.

### 4. Schema Registry Optimization

- **Cache hits**: Monitor `SinkStats.sr_hit_rate()`. Aim for >99% after warm-up
- **Subject strategy**: Use `"topic_name"` for Materialize compatibility
- **Warm-up**: Pre-register schemas during application startup

### 5. Network Considerations

- **`max_in_flight`**: Set based on network RTT. Higher values for high-latency connections
- **`request_timeout_ms`**: Set to 2-3x your typical broker response time
- **Connection pooling**: librdkafka manages connections automatically

## Monitoring and Debugging

### Key Metrics to Monitor

```python
stats = sink.stats()
print(f"Rows enqueued: {stats.enqueued_total}")
print(f"Flush count: {stats.flush_count}")
print(f"Schema Registry hit rate: {stats.sr_hit_rate():.1%}")
```

### Common Issues and Solutions

#### Issue: High Latency
- **Check**: `linger_ms` too high, `batch_size` too large
- **Fix**: Reduce `linger_ms`, use smaller `batch_size`

#### Issue: Low Throughput
- **Check**: Compression too aggressive, `max_in_flight` too low
- **Fix**: Use faster compression (lz4/snappy), increase `max_in_flight`

#### Issue: Schema Registry Bottleneck
- **Check**: Low `sr_hit_rate()`
- **Fix**: Reuse sinks, pre-register schemas

#### Issue: Memory Growth
- **Check**: Large `batch_size` with many topics
- **Fix**: Reduce `batch_size`, implement backpressure

### Performance Testing Methodology

1. **Baseline**: Start with default settings
2. **Isolate variables**: Change one parameter at a time
3. **Measure**: Use `SinkStats` to track enqueued rows per second
4. **Validate**: Ensure flush() success rate remains high
5. **Iterate**: Adjust based on application requirements

## Advanced Topics

### 1. Multi-threaded Production

For maximum throughput, use multiple `ArrowKafkaSink` instances:

```python
import threading
from concurrent.futures import ThreadPoolExecutor

def produce_chunk(sink: ArrowKafkaSink, chunk: pa.Table):
    sink.consume_arrow(chunk, topic="events")
    sink.flush(5000)

# Create multiple sinks (each has its own connection pool)
sinks = [ArrowKafkaSink(...) for _ in range(4)]

with ThreadPoolExecutor(max_workers=4) as executor:
    futures = []
    for i, chunk in enumerate(chunks):
        future = executor.submit(produce_chunk, sinks[i % 4], chunk)
        futures.append(future)
    
    # Wait for completion
    for future in futures:
        future.result()
```

### 2. Dynamic Configuration

Adjust settings based on workload:

```python
def create_sink_for_workload(workload_type: str) -> ArrowKafkaSink:
    if workload_type == "realtime":
        return ArrowKafkaSink(linger_ms=0, batch_size=16384, ...)
    elif workload_type == "batch":
        return ArrowKafkaSink(linger_ms=100, batch_size=1048576, ...)
    else:
        return ArrowKafkaSink()  # Defaults
```

### 3. Integration with Observability

```python
import time
import logging

logger = logging.getLogger(__name__)

def produce_with_metrics(sink: ArrowKafkaSink, table: pa.Table, topic: str):
    start_time = time.time()
    rows = sink.consume_arrow(table, topic=topic)
    sink.flush(10000)
    end_time = time.time()
    
    duration = end_time - start_time
    rows_per_sec = rows / duration if duration > 0 else 0
    
    logger.info(
        f"Produced {rows} rows to {topic} in {duration:.2f}s "
        f"({rows_per_sec:.0f} rows/sec)"
    )
```

## References

- [librdkafka Configuration Documentation](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)
- [Confluent Producer Tuning Guide](https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html)
- [Materialize Performance Guide](https://materialize.com/docs/performance/)
- [Avro Compression Benchmarks](https://github.com/lz4/lz4)

## Support

For additional tuning assistance:
1. Check the `SinkStats` output for bottlenecks
2. Monitor Kafka broker metrics (especially queue sizes)
3. Review Schema Registry logs for errors
4. Consider network latency between producer and broker