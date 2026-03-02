# Arrow-Kafka-Pyo3

<a href="README.zh-CN.md">简体中文</a> | <a href="docs/en/getting-started.md">Quick Start</a> | <a href="docs/en/user-guide.md">User Guide</a>

High-performance Kafka sink with Arrow zero-copy support for financial data, real-time streaming, and batch processing scenarios.

## 🚀 Key Features

### ✅ Production-Ready
- **Structured error handling**: 7 dedicated exception classes with clear error context
- **Complete type support**: Covers all financial Arrow types (Date32, Timestamp, Decimal128, etc.)
- **Reliability configuration**: Supports idempotent production, exactly-once semantics
- **Observability**: Built-in statistics counters, monitoring cache hit rate and throughput

### 🔧 Core Capabilities
- **Zero-copy**: Direct Arrow FFI from `pyarrow.Table` to Avro, no memory copying
- **Schema Registry integration**: Supports Confluent/Redpanda Schema Registry
- **Materialize compatible**: Uses Confluent wire format, directly compatible with Materialize

## 📦 Installation

### From PyPI (Recommended)
```bash
pip install arrow-kafka-pyo3
```

### From Source
```bash
git clone https://github.com/your-org/arrow-kafka.git
cd arrow-kafka

# Install Rust toolchain if needed
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Build Python extension
cd crates/arrow-kafka-pyo3
maturin develop
```

## 🚀 5-Minute Quick Start

```python
import pyarrow as pa
from arrow_kafka_pyo3 import ArrowKafkaSink

# Create sink instance
sink = ArrowKafkaSink(
    kafka_servers="localhost:9092",
    schema_registry_url="http://localhost:8081",
)

# Prepare data
table = pa.table({
    "symbol": ["AAPL", "GOOGL", "MSFT"],
    "price": [189.3, 2750.5, 342.8],
    "volume": [1000, 500, 1200]
})

# Send data
rows_sent = sink.consume_arrow(
    table=table,
    topic="stock_quotes",
    key_cols=["symbol"]
)

print(f"✅ Sent {rows_sent} rows to Kafka")

# Ensure delivery
sink.flush(timeout_ms=10000)

# Close connection
sink.close()
```

For more detailed examples, see <a href="docs/en/getting-started.md">Getting Started Guide</a>.

## 📚 Documentation

### Quick Navigation
- **[Getting Started](docs/en/getting-started.md)** - Installation and first steps
- **[Complete User Guide](docs/en/user-guide.md)** - Comprehensive usage guide with examples
- **[API Reference](docs/en/api-reference.md)** - Detailed API documentation
- **[Schema Evolution](docs/en/schema-evolution.md)** - Schema compatibility rules
- **[FAQ](docs/en/faq.md)** - Common questions and troubleshooting
- **[中文文档](README.zh-CN.md)** - Complete documentation in Chinese

### Topics Covered
- Performance tuning and configuration presets
- Production deployment and monitoring
- Error handling and exception hierarchy
- Kafka headers and topic administration
- Materialize integration examples

## 🔧 Advanced Configuration Example

```python
sink = ArrowKafkaSink(
    kafka_servers="kafka1:9092,kafka2:9092,kafka3:9092",
    schema_registry_url="http://schema-registry:8081",
    
    # Reliability
    enable_idempotence=True,
    acks="all",
    retries=10,
    
    # Performance
    linger_ms=20,
    batch_size=65536,
    compression_type="lz4",
    
    # Schema Registry
    subject_name_strategy="topic_name",  # Materialize compatible
)
```

## 📊 Monitoring

```python
stats = sink.stats()
print(f"Rows enqueued: {stats.enqueued_total}")
print(f"Cache hit rate: {stats.sr_hit_rate():.1%}")
print(f"Cache hits: {stats.sr_cache_hits}, misses: {stats.sr_cache_misses}")
```

See <a href="docs/en/user-guide.md">User Guide</a> for detailed monitoring instructions.

## 🧪 Testing

```bash
# Rust tests
cargo test -p arrow-kafka

# Python tests (requires built extension)
cd crates/arrow-kafka-pyo3 && maturin develop
python -m pytest tests/ -v
```

## 📈 Performance Benchmarks

| Scenario | Throughput | Latency | Memory |
|----------|------------|---------|---------|
| Low latency mode | 10-100 MB/s | 1-10ms | Low |
| High throughput mode | 500 MB/s+ | 20-100ms | Medium |
| Exactly-once mode | 100-300 MB/s | 10-50ms | Low |

## 🤝 Contributing

We welcome issues and pull requests! See [CONTRIBUTING.md](CONTRIBUTING.md) for details.

### Development Setup
```bash
rustup install stable
pip install -r requirements-dev.txt
pre-commit install
```

## 📄 License

MIT License - see [LICENSE](LICENSE) for details.

## 🙏 Acknowledgments

- [librdkafka](https://github.com/edenhill/librdkafka) - Reliable Kafka client
- [Apache Arrow](https://arrow.apache.org/) - Zero-copy data exchange
- [Materialize](https://materialize.com/) - Real-time data warehouse

## 📞 Support

- **GitHub Issues**: [Report bugs or request features](https://github.com/your-org/arrow-kafka/issues)
- **Documentation**: Submit improvements via pull requests

---

**Arrow-Kafka-Pyo3** - High-performance Kafka data sink for production environments