# Arrow-Kafka-Pyo3 Getting Started

<a href="../zh-CN/getting-started.md">简体中文</a> | <a href="user-guide.md">User Guide</a> | <a href="api-reference.md">API Reference</a>

## 🎯 Project Overview

Arrow-Kafka-Pyo3 is a high-performance Kafka sink library with Arrow zero-copy support, designed for financial data, real-time streaming, and batch processing scenarios.

### Key Features

- **Zero-copy**: Direct Arrow FFI from `pyarrow.Table` to Avro, no memory copying
- **Complete type support**: All financial Arrow types (Date32, Timestamp, Decimal128, etc.)
- **Structured error handling**: 7 dedicated exception classes with clear error context
- **Production-ready**: Idempotent production, exactly-once semantics, configurable retry policies
- **Materialize compatible**: Uses Confluent wire format, directly compatible with Materialize

## 📦 Installation

### From PyPI (Recommended)

```bash
pip install arrow-kafka-pyo3
```

### From Source (Development)

```bash
# Clone the project
git clone https://github.com/your-org/arrow-kafka.git
cd arrow-kafka

# Install Rust toolchain (if not already installed)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Build Python extension
cd crates/arrow-kafka-pyo3
maturin develop
```

### Requirements

- **Python**: 3.8 or higher
- **pyarrow**: 8.0.0 or higher
- **Kafka/Redpanda**: For data production (you can use Docker for testing)

## 🚀 5-Minute Quick Start

### 1. Start Test Environment

```bash
# Start Redpanda and Schema Registry (using Docker)
docker run -d \
  --name redpanda \
  -p 9092:9092 \
  -p 8081:8081 \
  docker.redpanda.com/redpandadata/redpanda:v23.2.9 \
  redpanda start \
  --smp 1 \
  --memory 1G \
  --reserve-memory 0M \
  --overprovisioned \
  --node-id 0 \
  --kafka-addr PLAINTEXT://0.0.0.0:9092 \
  --advertise-kafka-addr PLAINTEXT://redpanda:9092 \
  --schema-registry-addr 0.0.0.0:8081
```

### 2. Basic Usage Example

```python
import pyarrow as pa
import pandas as pd
from arrow_kafka_pyo3 import ArrowKafkaSink

# Create Sink instance
sink = ArrowKafkaSink(
    kafka_servers="localhost:9092",
    schema_registry_url="http://localhost:8081",
)

# Prepare test data
data = {
    "symbol": ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"],
    "price": [189.3, 2750.5, 342.8, 155.2, 248.8],
    "volume": [1000, 500, 1200, 800, 1500],
    "timestamp": pd.date_range("2024-01-01 10:00:00", periods=5, freq="1s")
}

table = pa.table(data)

# Send data to Kafka
try:
    rows_sent = sink.consume_arrow(
        table=table,
        topic="stock_quotes",
        key_cols=["symbol"],  # Use symbol as Kafka key
        timeout_ms=5000       # 5-second timeout
    )
    print(f"✅ Successfully sent {rows_sent} rows")
    
    # Wait for all messages to be acknowledged by Kafka
    sink.flush(timeout_ms=10000)
    print("✅ All messages acknowledged by Kafka broker")
    
except Exception as e:
    print(f"❌ Send failed: {e}")

# Close connection
sink.close()
print("✅ Connection closed")
```

### 3. Run the Example

```bash
python basic_example.py
```

## 📊 Monitoring and Statistics

```python
import time
from arrow_kafka_pyo3 import ArrowKafkaSink

sink = ArrowKafkaSink(
    kafka_servers="localhost:9092",
    schema_registry_url="http://localhost:8081",
)

# Get runtime statistics
stats = sink.stats()
print("📊 Initial statistics:")
print(f"  Total rows enqueued: {stats.enqueued_total}")
print(f"  Flush count: {stats.flush_count}")
print(f"  Schema Registry cache hit rate: {stats.sr_hit_rate():.1%}")

# Send some data and check again
import pyarrow as pa
table = pa.table({"test": [1, 2, 3]})
sink.consume_arrow(table, topic="test_topic")
sink.flush()

stats = sink.stats()
print("\n📊 Statistics after sending data:")
print(f"  Total rows enqueued: {stats.enqueued_total}")
print(f"  Cache hits: {stats.sr_cache_hits}, Cache misses: {stats.sr_cache_misses}")

sink.close()
```

## ⚙️ Basic Configuration

```python
sink = ArrowKafkaSink(
    # Required parameters
    kafka_servers="host1:9092,host2:9092,host3:9092",  # Kafka cluster addresses
    schema_registry_url="http://schema-registry:8081",  # Schema Registry URL
    
    # Performance parameters (optional)
    linger_ms=20,           # Batch waiting time (milliseconds)
    batch_size=65536,       # Batch size (bytes)
    compression_type="lz4", # Compression: none, gzip, snappy, lz4, zstd
    max_in_flight=1000,     # Max in-flight requests per connection
    
    # Reliability parameters (optional)
    enable_idempotence=False,  # Enable idempotent production (exactly-once)
    acks="1",                 # Acknowledgement level: 0, 1, all
    retries=None,             # Retry count, None for infinite
    request_timeout_ms=30000, # Request timeout (ms)
)
```

### Configuration Presets

#### Low Latency Mode (Real-time Trading)

```python
low_latency_sink = ArrowKafkaSink(
    linger_ms=0,           # No batching delay
    batch_size=16384,      # Small batches
    compression_type="none", # No compression
    enable_idempotence=True, # Exactly-once semantics
    acks="1",              # Leader acknowledgement
    max_in_flight=5,       # Small window (required for idempotence)
)
```

#### High Throughput Mode (Batch Processing)

```python
high_throughput_sink = ArrowKafkaSink(
    linger_ms=100,         # 100ms batching
    batch_size=1048576,    # 1MB batches
    compression_type="lz4", # LZ4 compression
    enable_idempotence=False, # Disable idempotence (higher throughput)
    acks="0",              # No acknowledgement (fastest)
    max_in_flight=1000,    # Large window
)
```

## 🚨 Error Handling

Arrow-Kafka-Pyo3 provides structured error handling:

```python
from arrow_kafka_pyo3 import (
    ArrowKafkaSink,
    SchemaRegistryError,
    EnqueueError,
    FlushTimeoutError
)

sink = ArrowKafkaSink(...)

try:
    # Try to send data
    rows_sent = sink.consume_arrow(table, topic="data", timeout_ms=1000)
    sink.flush(timeout_ms=5000)
    
except SchemaRegistryError as e:
    print(f"Schema Registry error: {e}")
    # Check network connectivity or schema compatibility
    
except EnqueueError as e:
    rows_done = e.args[1]  # Get rows already enqueued
    print(f"Partial success: {rows_done} rows enqueued")
    # Can retry remaining rows
    
except FlushTimeoutError as e:
    print(f"Flush timeout: {e}")
    # Can retry flush or accept potential data loss
```

## 🔧 Advanced Features

### 1. Kafka Headers Support

```python
headers = {
    "trace_id": b"abc-123-xyz",
    "source": b"trading_system",
    "version": b"v2.0"
}

sink.consume_arrow(
    table=table,
    topic="events",
    headers=headers
)
```

### 2. Topic Administration

```python
from arrow_kafka_pyo3 import create_topic_if_not_exists

# Ensure topic exists
created = create_topic_if_not_exists(
    bootstrap_servers="localhost:9092",
    topic="new_events",
    num_partitions=6,
    replication_factor=1,
    timeout_ms=10000
)

if created:
    print("Topic created")
else:
    print("Topic already exists")
```

## 🧪 Test Your Installation

Create a simple test script to verify installation:

```python
import pyarrow as pa
from arrow_kafka_pyo3 import ArrowKafkaSink, __version__

print(f"✅ Arrow-Kafka-Pyo3 version: {__version__}")

# Test basic functionality
try:
    sink = ArrowKafkaSink(
        kafka_servers="localhost:9092",
        schema_registry_url="http://localhost:8081",
    )
    print("✅ Sink created successfully")
    
    table = pa.table({"test": [1, 2, 3]})
    print("✅ Arrow table created successfully")
    
    # Note: This will fail if Kafka is not running
    # rows = sink.consume_arrow(table, topic="test_topic")
    # print(f"✅ Data sent successfully: {rows} rows")
    
    sink.close()
    print("✅ All tests passed!")
    
except Exception as e:
    print(f"❌ Test failed: {e}")
```

## 📚 Next Steps

### Learn More

1. **Read Complete Documentation**:
   - **[User Guide](user-guide.md)** - Detailed usage instructions and best practices
   - **[Performance Tuning](user-guide.md#performance-tuning)** - Configuration presets and optimization
   - **[Schema Evolution](schema-evolution.md)** - Schema compatibility rules

2. **Check Example Code**:
   ```bash
   # View examples in the project
   find examples/ -name "*.py" -type f
   ```

3. **Run Benchmarks**:
   ```bash
   python tests/benchmark/bench_arrow_kafka.py
   ```

### Prepare for Production

1. **Performance Testing**: Run benchmarks with your data
2. **Monitor Integration**: Integrate `SinkStats` into your monitoring system
3. **Error Handling**: Implement appropriate retry and fallback strategies
4. **Read Documentation**: Check the [User Guide](user-guide.md) for production recommendations

## ❓ Frequently Asked Questions

### Q: What if I get "Connection refused" error?
**A**: Make sure Kafka/Redpanda is running and accessible from your application:
```bash
# Check if Kafka is running
nc -z localhost 9092 && echo "Kafka available" || echo "Kafka unavailable"

# Check Schema Registry
curl -f http://localhost:8081 && echo "Schema Registry available" || echo "Schema Registry unavailable"
```

### Q: How do I choose the right compression algorithm?
**A**: Refer to these recommendations:
- **Low latency**: `none` or `snappy`
- **General purpose**: `lz4` (balanced speed and compression)
- **Bandwidth constrained**: `zstd` or `gzip`

### Q: Why do I need to call `flush()`?
**A**: `consume_arrow()` only guarantees data is **enqueued** to the send buffer. `flush()` ensures data is **acknowledged** by Kafka brokers. In production, regular `flush()` calls are important.

## 📞 Getting Help

- **GitHub Issues**: [Report issues or request features](https://github.com/your-org/arrow-kafka/issues)
- **Documentation Issues**: Submit documentation improvements via pull requests
- **Community Support**: Check project discussions

---

*Last updated: January 2024*  
*Version: 1.0.0*