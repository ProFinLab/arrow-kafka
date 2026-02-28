"""
arrow-kafka-pyo3 performance benchmark.
Requires: docker compose up -d, maturin develop in crates/arrow-kafka-pyo3.
Run: python tests/bench/bench_arrow_kafka.py
"""

from __future__ import annotations

import random
import string
import time
from dataclasses import dataclass

import pyarrow as pa

from arrow_kafka_pyo3 import ArrowKafkaSink

BOOTSTRAP = "localhost:9092"
SR_URL = "http://localhost:18081"


def gen_stock_table(n_rows: int) -> pa.Table:
    symbols = [
        f"{''.join(random.choices(string.digits, k=6))}.{'SZ' if random.random() < 0.5 else 'SH'}"
        for _ in range(n_rows)
    ]
    dates = [20240101 + (i % 365) for i in range(n_rows)]

    def rand_f64():
        return [round(random.uniform(5.0, 500.0), 2) for _ in range(n_rows)]

    return pa.table(
        {
            "symbol": symbols,
            "trade_date": pa.array(dates, type=pa.int64()),
            "open": pa.array(rand_f64(), type=pa.float64()),
            "high": pa.array(rand_f64(), type=pa.float64()),
            "low": pa.array(rand_f64(), type=pa.float64()),
            "close": pa.array(rand_f64(), type=pa.float64()),
            "volume": pa.array(
                [random.randint(100_000, 50_000_000) for _ in range(n_rows)],
                type=pa.int64(),
            ),
        }
    )


def gen_wide_table(n_rows: int, n_float_cols: int = 50) -> pa.Table:
    data = {"id": pa.array(list(range(n_rows)), type=pa.int64())}
    for i in range(n_float_cols):
        data[f"factor_{i:03d}"] = pa.array(
            [random.gauss(0, 1) for _ in range(n_rows)], type=pa.float64()
        )
    return pa.table(data)


@dataclass
class BenchResult:
    name: str
    n_rows: int
    n_cols: int
    table_bytes: int
    send_sec: float
    flush_sec: float
    rows_per_sec: float
    mb_per_sec: float

    def __str__(self) -> str:
        return (
            f"  {self.name:<35s} | "
            f"{self.n_rows:>10,d} rows × {self.n_cols:>3d} cols | "
            f"send {self.send_sec:6.3f}s  flush {self.flush_sec:6.3f}s | "
            f"{self.rows_per_sec:>12,.0f} rows/s | "
            f"{self.mb_per_sec:>8.2f} MB/s"
        )


def table_nbytes(t: pa.Table) -> int:
    return sum(c.nbytes for c in t.columns)


def run_one(
    name: str,
    table: pa.Table,
    topic: str,
    *,
    key_cols: list[str] | None = None,
    compression: str = "none",
    linger_ms: int = 5,
    batch_size: int = 1_000_000,
) -> BenchResult:
    sink = ArrowKafkaSink(
        kafka_servers=BOOTSTRAP,
        schema_registry_url=SR_URL,
        linger_ms=linger_ms,
        batch_size=batch_size,
        compression_type=compression,
    )
    small = table.slice(0, min(10, table.num_rows))
    sink.consume_arrow(small, topic=topic)
    sink.flush()

    t0 = time.perf_counter()
    sent = sink.consume_arrow(
        table,
        topic=topic,
        key_cols=key_cols,
        key_separator="_",
    )
    t_send = time.perf_counter() - t0
    t1 = time.perf_counter()
    sink.flush()
    t_flush = time.perf_counter() - t1
    total = t_send + t_flush
    nbytes = table_nbytes(table)
    return BenchResult(
        name=name,
        n_rows=sent,
        n_cols=table.num_columns,
        table_bytes=nbytes,
        send_sec=t_send,
        flush_sec=t_flush,
        rows_per_sec=sent / total if total > 0 else 0,
        mb_per_sec=(nbytes / 1024 / 1024) / total if total > 0 else 0,
    )


def main() -> None:
    print("=" * 120)
    print("arrow-kafka-pyo3 Performance Benchmark")
    print(f"  PyArrow {pa.__version__}  |  Redpanda @ {BOOTSTRAP}  |  SR @ {SR_URL}")
    print("=" * 120)
    all_results: list[BenchResult] = []

    print("\n── 1. Scale ──")
    for n in [1_000, 10_000, 100_000]:
        table = gen_stock_table(n)
        topic = f"bench_scale_{n}"
        r = run_one(f"stock_7col_{n // 1000}K", table, topic, key_cols=["symbol"])
        all_results.append(r)
        print(r)

    print("\n── 2. Key overhead ──")
    table = gen_stock_table(100_000)
    for label, keys in [
        ("no_key", None),
        ("single_key", ["symbol"]),
        ("composite_key", ["symbol", "trade_date"]),
    ]:
        topic = f"bench_key_{label}"
        r = run_one(f"100K_{label}", table, topic, key_cols=keys)
        all_results.append(r)
        print(r)

    print("\n" + "=" * 120)
    peak = max(all_results, key=lambda r: r.rows_per_sec)
    print(
        f"  Peak: {peak.rows_per_sec:,.0f} rows/s  ({peak.mb_per_sec:.2f} MB/s)  [{peak.name}]"
    )
    print("=" * 120)


if __name__ == "__main__":
    main()
