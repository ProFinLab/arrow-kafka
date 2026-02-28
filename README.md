# arrow-kafka

Standalone project: high-performance Kafka sink with Arrow zero-copy support.  
Pure Rust core ([`crates/arrow-kafka`](crates/arrow-kafka)) + Python bindings ([`crates/arrow-kafka-pyo3`](crates/arrow-kafka-pyo3)).

## Layout

- `crates/arrow-kafka` — Rust library: Arrow → Avro, Schema Registry, rdkafka producer.
- `crates/arrow-kafka-pyo3` — PyO3 extension: `arrow_kafka_pyo3.ArrowKafkaSink`, zero-copy from `pyarrow.Table`.

## Build & test

First run requires network to fetch crates (`cargo check` or `cargo build`).

```bash
# Rust
cargo check
cargo test -p arrow-kafka

# Python extension (from repo root)
cd crates/arrow-kafka-pyo3 && maturin develop

# Integration tests (need Redpanda: docker compose up -d)
pip install -r requirements-dev.txt
pytest tests/integration -v

# Benchmark (optional)
python tests/bench/bench_arrow_kafka.py
```

## Python usage

```python
import pyarrow as pa
from arrow_kafka_pyo3 import ArrowKafkaSink

sink = ArrowKafkaSink(
    kafka_servers="localhost:9092",
    schema_registry_url="http://localhost:8081",
)
table = pa.table({"a": [1, 2], "b": [1.0, 2.0]})
sink.consume_arrow(table, topic="my_topic")
sink.flush()
sink.close()
```

## License

MIT
