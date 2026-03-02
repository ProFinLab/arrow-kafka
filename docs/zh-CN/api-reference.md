# API Reference

> **Note**: This API reference is under construction. For now, please refer to the [Python type stubs](../crates/arrow-kafka-pyo3/arrow_kafka_pyo3.pyi) and the [User Guide](user-guide.md).

## Overview

Arrow-Kafka-Pyo3 provides a Python interface for sending Apache Arrow data to Kafka with zero-copy efficiency. The main classes and functions are documented below.

## Main Classes

### `ArrowKafkaSink`

The primary class for sending Arrow data to Kafka.

#### Constructor
```python
ArrowKafkaSink(
    kafka_servers: str,
    schema_registry_url: str,
    # Configuration parameters...
)
```

#### Methods
- `consume_arrow()` - Send Arrow table to Kafka
- `flush()` - Ensure all messages are acknowledged
- `close()` - Close the sink connection
- `stats()` - Get runtime statistics

### `SinkStats`

Statistics class for monitoring sink performance.

#### Attributes
- `enqueued_total` - Total rows enqueued
- `flush_count` - Number of flush calls
- `sr_cache_hits` - Schema Registry cache hits
- `sr_cache_misses` - Schema Registry cache misses

#### Methods
- `sr_hit_rate()` - Calculate cache hit rate

## Utility Functions

### `create_topic_if_not_exists()`

Create a Kafka topic if it doesn't already exist.

```python
create_topic_if_not_exists(
    bootstrap_servers: str,
    topic: str,
    num_partitions: int = 1,
    replication_factor: int = 1,
    timeout_ms: int = 10000
) -> bool
```

## Exception Hierarchy

Arrow-Kafka-Pyo3 defines the following exception classes:

- `ArrowKafkaError` - Base exception class
- `SchemaRegistryError` - Schema Registry related errors
- `EnqueueError` - Error during data enqueue
- `FlushTimeoutError` - Flush operation timeout
- `KafkaConnectionError` - Kafka connection issues
- `InvalidConfigurationError` - Invalid configuration
- `SerializationError` - Data serialization errors

## Type Support

### Supported Arrow Types
- String types: `Utf8`, `LargeUtf8`
- Integer types: `Int8`, `Int16`, `Int32`, `Int64`, `UInt8`, `UInt16`, `UInt32`, `UInt64`
- Floating-point types: `Float32`, `Float64`
- Boolean type: `Boolean`
- Temporal types: `Date32`, `Date64`, `Timestamp` (all units)
- Binary types: `Binary`, `LargeBinary`, `FixedSizeBinary`
- Decimal types: `Decimal128`

## Configuration Parameters

### Required Parameters
- `kafka_servers` - Kafka bootstrap servers (comma-separated)
- `schema_registry_url` - Schema Registry URL

### Performance Parameters
- `linger_ms` - Batch waiting time (default: 20)
- `batch_size` - Batch size in bytes (default: 65536)
- `compression_type` - Compression codec (default: "none")
- `max_in_flight` - Max in-flight requests (default: 1000)

### Reliability Parameters
- `enable_idempotence` - Enable idempotent production (default: False)
- `acks` - Acknowledgement level (default: "1")
- `retries` - Retry count (default: None)
- `request_timeout_ms` - Request timeout (default: 30000)

### Schema Registry Parameters
- `subject_name_strategy` - Subject naming strategy (default: "topic_name")

## Examples

### Basic Usage
```python
from arrow_kafka_pyo3 import ArrowKafkaSink
import pyarrow as pa

sink = ArrowKafkaSink(
    kafka_servers="localhost:9092",
    schema_registry_url="http://localhost:8081"
)

table = pa.table({"id": [1, 2, 3], "value": ["a", "b", "c"]})
rows_sent = sink.consume_arrow(table, topic="test_topic")
sink.flush()
sink.close()
```

## Next Steps

For complete API documentation, please:
1. Check the [Python type stubs](../crates/arrow-kafka-pyo3/arrow_kafka_pyo3.pyi)
2. Read the [User Guide](user-guide.md) for detailed examples
3. Review the [Getting Started](getting-started.md) guide for basics

---

*Last updated: January 2024*  
*Version: 1.0.0*