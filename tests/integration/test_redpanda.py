"""
Integration test: ArrowKafkaSink produce to Redpanda, then consume back.
Requires: docker compose up -d (from repo root), maturin develop in crates/arrow-kafka-pyo3.
"""
from __future__ import annotations

import time

import pyarrow as pa
from confluent_kafka import Consumer, KafkaException
from arrow_kafka_pyo3 import ArrowKafkaSink

TOPIC = "arrow_kafka_test"
BOOTSTRAP_SERVERS = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:18081"


def _make_sink() -> ArrowKafkaSink:
    return ArrowKafkaSink(
        kafka_servers=BOOTSTRAP_SERVERS,
        schema_registry_url=SCHEMA_REGISTRY_URL,
        max_in_flight=1000,
        linger_ms=20,
        batch_size=65536,
        compression_type="none",
    )


def _make_arrow_table() -> pa.Table:
    return pa.table(
        {
            "symbol": ["000001.SZ", "000002.SZ"],
            "trade_date": pa.array([20240101, 20240101], type=pa.int64()),
            "close": pa.array([10.5, 20.3], type=pa.float64()),
        }
    )


def test_arrow_sink_produces_to_redpanda() -> None:
    table = _make_arrow_table()
    sink = _make_sink()

    produced = sink.consume_arrow(
        table,
        topic=TOPIC,
        key_cols=["symbol", "trade_date"],
        key_separator="_",
    )
    assert produced == table.num_rows

    sink.flush()

    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": "arrow-kafka-test",
            "auto.offset.reset": "earliest",
        }
    )
    try:
        consumer.subscribe([TOPIC])
        messages = []
        deadline = time.time() + 10.0

        while len(messages) < table.num_rows and time.time() < deadline:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            messages.append(msg)

        assert len(messages) == table.num_rows
    finally:
        consumer.close()
