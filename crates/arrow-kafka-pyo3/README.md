# arrow-kafka-pyo3

Python extension providing `ArrowKafkaSink`: send `pyarrow.Table` to Kafka/Redpanda with zero-copy Arrow FFI.

## Install

From PyPI (once published):

```bash
pip install arrow-kafka-pyo3
```

From source (repo root = parent of `crates/`):

```bash
cd crates/arrow-kafka-pyo3 && maturin develop
```

## Usage

```python
import pyarrow as pa
from arrow_kafka_pyo3 import ArrowKafkaSink

sink = ArrowKafkaSink(
    kafka_servers="localhost:9092",
    schema_registry_url="http://localhost:8081",
)
table = pa.table({"a": [1, 2], "b": [1.0, 2.0]})
sink.consume_arrow(table, topic="my_topic", key_cols=["a"])
sink.flush()
sink.close()
```

## Publish (maintainers)

```bash
cd crates/arrow-kafka-pyo3
maturin build --release
# TestPyPI: maturin upload -r testpypi target/wheels/*.whl
# PyPI:     maturin upload target/wheels/*.whl
```
