# Arrow-Kafka-Pyo3 User Guide

<center>
<strong>⚠️ Under Construction</strong><br>
This English user guide is currently being written.<br>
For now, please refer to the <a href="../zh-CN/user-guide.md">Chinese user guide</a> or the <a href="getting-started.md">Getting Started guide</a>.
</center>

## 📚 Table of Contents

1. [Project Overview](#project-overview)
2. [Installation Guide](#installation-guide)
3. [Quick Start](#quick-start)
4. [Core Concepts](#core-concepts)
5. [API Reference](#api-reference)
6. [Configuration Parameters](#configuration-parameters)
7. [Error Handling](#error-handling)
8. [Performance Tuning](#performance-tuning)
9. [Best Practices](#best-practices)
10. [Frequently Asked Questions](#frequently-asked-questions)

## 🎯 Project Overview

Arrow-Kafka-Pyo3 is a high-performance Kafka sink library with Arrow zero-copy support, designed for financial data, real-time streaming, and batch processing scenarios.

### Key Features

- **Zero-copy**: Direct Arrow FFI from `pyarrow.Table` to Avro, no memory copying
- **Complete type support**: All financial Arrow types (Date32, Timestamp, Decimal128, etc.)
- **Structured error handling**: 7 dedicated exception classes with clear error context
- **Production-ready**: Idempotent production, exactly-once semantics, configurable retry policies
- **Materialize compatible**: Uses Confluent wire format, directly compatible with Materialize
- **Observability**: Built-in statistics counters, monitoring cache hit rate and throughput

### Architecture Design

```
Data flow: pyarrow.Table → Arrow FFI → Rust core → Avro serialization → Kafka
Architecture layers:
  - Python binding layer: User-friendly Python API, GIL handling, exception conversion
  - Rust core layer: High-performance Arrow → Avro conversion, Schema Registry integration
  - librdkafka: Mature Kafka client, network communication and reliability
```

### Use Cases

| Scenario | Recommended Configuration | Key Features |
|----------|--------------------------|--------------|
| **Real-time trading systems** | Low latency mode | Millisecond latency, exactly-once semantics |
| **Data warehouse ETL** | High throughput mode | Batch compression, high throughput |
| **Log aggregation** | Default configuration | Balanced latency and throughput |
| **Financial data pipelines** | Reliability mode | Idempotent production, full monitoring |

## 📦 Installation Guide

### Environment Requirements

- **Python**: 3.8 or higher
- **Rust**: 1.70 or higher (for source builds)
- **Kafka**: 2.8+ or Redpanda (recommended)
- **Schema Registry**: Confluent or Redpanda Schema Registry

### Installation Methods

#### 1. From PyPI (Recommended)

```bash
pip install arrow-kafka-pyo3
```

#### 2. From Source (Development Environment)

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

## 🚀 Quick Start

*This section is under construction. Please see the [Getting Started guide](getting-started.md) for basic usage examples.*

## 🧠 Core Concepts

### 1. Message Format
Arrow-Kafka-Pyo3 converts Apache Arrow tables to Avro format for compatibility with Schema Registry and Materialize.

### 2. Delivery Semantics
Supports three delivery semantics:
- At-most-once (`acks="0"`)
- At-least-once (`acks="1"`, default)
- Exactly-once (`enable_idempotence=True` with `acks="all"`)

### 3. Schema Registry Integration
Automatic schema registration and compatibility checking with Confluent Schema Registry.

### 4. Thread Safety and GIL
Sink instances are thread-safe and release the Python GIL during long-running operations.

## 📖 API Reference

*This section is under construction. Please see the [API reference](api-reference.md) for detailed documentation.*

## ⚙️ Configuration Parameters

### Kafka Connection Parameters
- `kafka_servers` - Kafka bootstrap servers (comma-separated)
- Additional security and network parameters

### Reliability Parameters
- `enable_idempotence` - Enable idempotent production
- `acks` - Acknowledgement level
- `retries` - Number of retry attempts
- `request_timeout_ms` - Request timeout

### Performance Parameters
- `linger_ms` - Batch waiting time
- `batch_size` - Batch size in bytes
- `compression_type` - Compression codec
- `max_in_flight` - Maximum in-flight requests

### Schema Registry Parameters
- `subject_name_strategy` - Subject naming strategy

## 🚨 Error Handling

Arrow-Kafka-Pyo3 provides a structured error hierarchy:

```
ArrowKafkaError (base exception)
├── SchemaRegistryError
├── EnqueueError
├── FlushTimeoutError
├── KafkaConnectionError
├── InvalidConfigurationError
└── SerializationError
```

## 🚀 Performance Tuning

*This section is under construction. Please see the [Performance Tuning guide](tuning.md) for detailed optimization recommendations.*

## 💡 Best Practices

### 1. Data Preparation
Preprocess data to ensure type compatibility and optimal serialization.

### 2. Connection Pool Management
Reuse sink instances for better performance and resource utilization.

### 3. Graceful Shutdown
Always call `close()` on sink instances during application shutdown.

### 4. Schema Management
Implement schema caching and pre-registration for better performance.

## ❓ Frequently Asked Questions

*This section is under construction. Please see the [FAQ](faq.md) for common questions and answers.*

## 🎯 Next Steps

1. **Read the Chinese Guide**: For now, the most comprehensive documentation is available in [Chinese](../zh-CN/user-guide.md)
2. **Check Examples**: Review the `examples/` directory for practical usage
3. **Run Benchmarks**: Use the benchmark scripts to test performance
4. **Production Deployment**: Refer to deployment guides for production setup

## 📞 Getting Help

- **GitHub Issues**: [Report issues or request features](https://github.com/your-org/arrow-kafka/issues)
- **Documentation Issues**: Submit improvements via pull requests
- **Community Support**: Check project discussions

---

*This document is under active development. Last updated: January 2024*  
*Version: 1.0.0*