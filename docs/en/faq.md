# Frequently Asked Questions (FAQ)

<a href="../zh-CN/faq.md">简体中文</a> | <a href="getting-started.md">Getting Started</a> | <a href="user-guide.md">User Guide</a>

> **Note**: This FAQ is under construction. Please check back soon for more questions and answers, or contribute by submitting a pull request.

## Installation & Setup

### Q: What are the system requirements for running arrow-kafka-pyo3?
**A**: The minimum requirements are:
- **Operating System**: Linux x86_64 (glibc 2.17+)
- **Python**: 3.8 or higher
- **pyarrow**: 8.0.0 or higher
- **Memory**: 4GB minimum, 16GB+ recommended for production
- **Kafka**: 2.8+ or Redpanda v23.2+
- **Schema Registry**: Confluent 7.0+ or Redpanda Schema Registry

### Q: How do I install arrow-kafka-pyo3 from source?
**A**: See the [Getting Started Guide](getting-started.md) for detailed installation instructions.

## Configuration & Performance

### Q: How do I choose the right compression algorithm?
**A**: Compression algorithm recommendations:
- **Low latency**: `none` or `snappy`
- **General purpose**: `lz4` (balanced speed and compression ratio)
- **Bandwidth constrained**: `zstd` or `gzip`

### Q: What's the difference between `acks="0"`, `acks="1"`, and `acks="all"`?
**A**: These control the broker acknowledgement level:
- `"0"`: Fire-and-forget (highest throughput, no delivery guarantees)
- `"1"`: Leader acknowledgement (good balance, default)
- `"all"`: Full ISR acknowledgement (maximum reliability, lower throughput)

### Q: When should I enable idempotence?
**A**: Enable `enable_idempotence=True` when you need exactly-once delivery semantics. This is particularly important for financial data and mission-critical applications. Note that this requires `max_in_flight ≤ 5`.

## Error Handling & Troubleshooting

### Q: I'm getting "Connection refused" errors. What should I check?
**A**: 
1. Verify Kafka brokers are running and accessible
2. Check network connectivity between your application and Kafka cluster
3. Confirm Schema Registry is running and accessible
4. Verify firewall rules allow connections on the required ports

### Q: Why do I need to call `flush()`?
**A**: `consume_arrow()` only guarantees data is **enqueued** to the send buffer. `flush()` ensures data is **acknowledged** by Kafka brokers. In production environments, regular `flush()` calls are important for data durability.

### Q: My application is using too much memory. What can I do?
**A**: Try these optimizations:
- Reduce `batch_size` parameter
- Decrease `linger_ms` to send data more frequently
- Call `flush()` more often to release internal buffers
- Monitor with `SinkStats` to identify bottlenecks

## Schema Registry & Materialize Integration

### Q: What's the best subject naming strategy for Materialize compatibility?
**A**: Use `subject_name_strategy="topic_name"` for maximum compatibility with Materialize. This creates subjects in the format `{topic}-value`.

### Q: How does schema evolution work with Materialize?
**A**: Materialize requires backward compatible schema changes. See the [Schema Evolution Guide](schema-evolution.md) for detailed compatibility rules and examples.

## Production Deployment

### Q: How should I monitor arrow-kafka-pyo3 in production?
**A**: Key metrics to monitor:
- `SinkStats.enqueued_total` - Total rows sent
- `SinkStats.sr_hit_rate()` - Schema Registry cache efficiency
- `SinkStats.flush_count` - Number of flush operations
- Kafka broker metrics (queue sizes, partition leader distribution)

### Q: Is arrow-kafka-pyo3 thread-safe?
**A**: Yes, `ArrowKafkaSink` instances are thread-safe and can be shared across multiple threads. However, for maximum throughput, consider using multiple sink instances in parallel.

### Q: How do I handle graceful shutdown?
**A**: Always call `close()` on your sink instances during application shutdown. Consider implementing signal handlers to ensure proper cleanup.

## Advanced Topics

### Q: Can I use arrow-kafka-pyo3 with multiple Kafka clusters?
**A**: Yes, create separate `ArrowKafkaSink` instances for each cluster. Each sink maintains its own connection pool and configuration.

### Q: Does arrow-kafka-pyo3 support SSL/TLS for Kafka connections?
**A**: Yes, SSL/TLS is supported through the underlying librdkafka configuration. You can pass SSL parameters via the configuration dictionary.

### Q: How do I handle backpressure when producers are faster than consumers?
**A**: Monitor Kafka broker metrics and adjust your production rate. Consider implementing application-level backpressure based on `SinkStats` and broker queue sizes.

## Contributing & Support

### Q: Where can I report bugs or request features?
**A**: Please use [GitHub Issues](https://github.com/your-org/arrow-kafka/issues) to report bugs or request features.

### Q: How can I contribute to the project?
**A**: See the [CONTRIBUTING.md](../CONTRIBUTING.md) file for contribution guidelines, including how to set up a development environment and submit pull requests.

### Q: Where can I get additional help?
**A**: 
- Read the [User Guide](user-guide.md) for detailed usage instructions
- Check the [API Reference](api-reference.md) for class and method documentation
- Review existing [GitHub Issues](https://github.com/your-org/arrow-kafka/issues) for similar problems
- Consider contributing improvements via pull requests

---

*Have a question not answered here?*  
Please [submit an issue](https://github.com/your-org/arrow-kafka/issues) or contribute by adding your question and answer via pull request.

*Last updated: January 2024*  
*Version: 1.0.0*