arrow-kafka\docs\zh\user_guide.md
```

# Arrow-Kafka-Pyo3 用户指南

## 📚 目录
1. [项目概述](#项目概述)
2. [安装指南](#安装指南)
3. [快速开始](#快速开始)
4. [核心概念](#核心概念)
5. [API 详细说明](#api-详细说明)
6. [配置参数详解](#配置参数详解)
7. [错误处理](#错误处理)
8. [性能调优](#性能调优)
9. [最佳实践](#最佳实践)
10. [常见问题](#常见问题)

## 🎯 项目概述

### 什么是 Arrow-Kafka-Pyo3？

Arrow-Kafka-Pyo3 是一个高性能的 Kafka 数据下沉库，专门为金融数据、实时流处理和批处理场景设计。它通过以下特性实现生产就绪：

- **零拷贝数据传输**：利用 Apache Arrow 的内存格式，避免 `pyarrow.Table` 与 Avro 之间的内存复制
- **完整的 Arrow 类型支持**：覆盖金融数据所需的所有数据类型（Date32、Timestamp、Decimal128 等）
- **结构化错误处理**：7 个专用的异常类，提供清晰的错误上下文
- **企业级可靠性**：支持幂等生产、精确一次语义、可配置的重试策略
- **Materialize 兼容**：使用 Confluent 线格式，直接兼容 Materialize 实时数据仓库

### 架构设计

```dev/null/architecture.md#L1-20
数据流: pyarrow.Table → Arrow FFI → Rust 核心 → Avro 序列化 → Kafka
架构分层:
  - Python 绑定层: 提供友好的 Python API，处理 GIL 和异常转换
  - Rust 核心层: 高性能的 Arrow → Avro 转换，Schema Registry 集成
  - librdkafka: 成熟的 Kafka 客户端，处理网络通信和可靠性
```

### 适用场景

| 场景 | 推荐配置 | 关键特性 |
|------|---------|----------|
| **实时交易系统** | 低延迟模式 | 毫秒级延迟，精确一次语义 |
| **数据仓库 ETL** | 高吞吐模式 | 批量压缩，高吞吐量 |
| **日志聚合** | 默认配置 | 平衡的延迟和吞吐 |
| **金融数据管道** | 可靠模式 | 幂等生产，完整监控 |

## 📦 安装指南

### 环境要求

- **Python**: 3.8 或更高版本
- **Rust**: 1.70 或更高版本（用于从源码构建）
- **Kafka**: 2.8+ 或 Redpanda（推荐）
- **Schema Registry**: Confluent 或 Redpanda Schema Registry

### 安装方式

#### 1. 从 PyPI 安装（推荐）

```bash
pip install arrow-kafka-pyo3
```

#### 2. 从源码安装（开发环境）

```bash
# 克隆项目
git clone https://github.com/your-org/arrow-kafka.git
cd arrow-kafka

# 安装 Rust 工具链（如未安装）
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# 构建 Python 扩展
cd crates/arrow-kafka-pyo3
maturin develop
```

#### 3. 验证安装

```python
# test_install.py
import pyarrow as pa
from arrow_kafka_pyo3 import ArrowKafkaSink

print("✅ Arrow-Kafka-Pyo3 安装成功！")
```

### Docker 环境设置

```dockerfile
# docker-compose.yml
version: '3.8'
services:
  redpanda:
    image: docker.redpanda.com/redpandadata/redpanda:v23.2.9
    command:
      - redpanda start
      - --smp 1
      - --memory 1G
      - --reserve-memory 0M
      - --overprovisioned
      - --node-id 0
      - --kafka-addr PLAINTEXT://0.0.0.0:29092,OUTSIDE://0.0.0.0:9092
      - --advertise-kafka-addr PLAINTEXT://redpanda:29092,OUTSIDE://localhost:9092
      - --pandaproxy-addr PLAINTEXT://0.0.0.0:28082,OUTSIDE://0.0.0.0:8082
      - --advertise-pandaproxy-addr PLAINTEXT://redpanda:28082,OUTSIDE://localhost:8082
      - --schema-registry-addr PLAINTEXT://0.0.0.0:8081,OUTSIDE://0.0.0.0:18081
    ports:
      - "9092:9092"
      - "8081:18081"
```

## 🚀 快速开始

### 基础示例

```python
# basic_usage.py
import pyarrow as pa
import pandas as pd
from arrow_kafka_pyo3 import ArrowKafkaSink

# 1. 创建 Sink 实例
sink = ArrowKafkaSink(
    kafka_servers="localhost:9092",
    schema_registry_url="http://localhost:8081",
)

# 2. 准备数据
data = {
    "symbol": ["AAPL", "GOOGL", "MSFT"],
    "price": [189.3, 2750.5, 342.8],
    "volume": [1000, 500, 1200],
    "timestamp": pd.date_range("2024-01-01 10:00:00", periods=3, freq="1s")
}

table = pa.table(data)

# 3. 发送数据
rows_sent = sink.consume_arrow(
    table=table,
    topic="stock_quotes",
    key_cols=["symbol"],  # 使用 symbol 作为 Kafka key
    key_separator="_",    # key 分隔符
    timeout_ms=5000       # 5秒超时
)

print(f"✅ 成功发送 {rows_sent} 行数据到 Kafka")

# 4. 等待所有消息被确认
sink.flush(timeout_ms=10000)
print("✅ 所有消息已被 Kafka 确认")

# 5. 关闭连接
sink.close()
```

### 生产环境示例

```python
# production_usage.py
from arrow_kafka_pyo3 import ArrowKafkaSink, create_topic_if_not_exists

# 1. 确保主题存在
create_topic_if_not_exists(
    bootstrap_servers="kafka1:9092,kafka2:9092,kafka3:9092",
    topic="financial_events",
    num_partitions=6,
    replication_factor=3,
    timeout_ms=10000
)

# 2. 创建生产级 Sink
sink = ArrowKafkaSink(
    kafka_servers="kafka1:9092,kafka2:9092,kafka3:9092",
    schema_registry_url="http://schema-registry:8081",
    
    # 可靠性配置
    enable_idempotence=True,    # 启用幂等生产（精确一次）
    acks="all",                 # 等待所有副本确认
    retries=10,                 # 最多重试10次
    retry_backoff_ms=100,       # 重试间隔100ms
    
    # 性能调优
    linger_ms=20,               # 20ms 批量等待
    batch_size=65536,           # 64KB 批次大小
    compression_type="lz4",     # LZ4 压缩（速度与压缩比平衡）
    max_in_flight=5,            # 幂等模式要求 ≤5
    
    # Schema Registry
    subject_name_strategy="topic_name",  # Materialize 兼容
)

# 3. 发送监控数据
stats = sink.stats()
print(f"初始状态: {stats}")
```

## 🧠 核心概念

### 1. 消息格式

Arrow-Kafka-Pyo3 使用 **Confluent 线格式**，每个消息的结构如下：

```dev/null/message_format.md#L1-10
[0x00][schema_id: u32 big-endian][avro_datum]
└── 魔法字节 └── 4字节模式ID └── Avro 数据体
```

这种格式的优点是：
- ✅ **Schema Registry 兼容**：模式 ID 嵌入消息中
- ✅ **Materialize 兼容**：可直接用于 `VALUE FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY`
- ✅ **模式演化支持**：消费者可以根据模式 ID 获取正确的模式

### 2. 交付语义

```dev/null/delivery_semantics.md#L1-15
consume_arrow() → 数据入队到 librdkafka 缓冲区 → 立即返回
flush() → 等待所有消息被 broker 确认 → 阻塞直到完成

关键区别：
  - 入队 ≠ 确认
  - 调用 flush() 前，消息可能仍在传输中
  - 超时控制：consume_arrow(timeout_ms) 和 flush(timeout_ms)
```

### 3. Schema Registry 集成

流程：
1. **第一次发送**：注册 Arrow Schema → Avro Schema，获取 Schema ID
2. **后续发送**：从内存缓存获取 Schema ID（无网络开销）
3. **监控**：通过 `SinkStats.sr_hit_rate()` 监控缓存效率

### 4. 线程安全和 GIL

- ✅ **线程安全**：`ArrowKafkaSink` 可以在多个线程间共享
- ✅ **GIL 释放**：`consume_arrow()` 和 `flush()` 在 Rust 代码执行期间释放 GIL
- ⚠️ **Python 对象**：`pyarrow.Table` 需要在每个线程中单独创建

## 📖 API 详细说明

### ArrowKafkaSink 类

#### 构造函数

```python
sink = ArrowKafkaSink(
    kafka_servers: str,
    schema_registry_url: str,
    max_in_flight: int = 1000,
    linger_ms: int = 20,
    batch_size: int = 65536,
    compression_type: str = "none",
    subject_name_strategy: str = "topic_name",
    enable_idempotence: bool = False,
    acks: str = "1",
    retries: Optional[int] = None,
    retry_backoff_ms: int = 100,
    request_timeout_ms: int = 30000
)
```

#### consume_arrow 方法

```python
def consume_arrow(
    self,
    table: pyarrow.Table,
    topic: str,
    key_cols: Optional[List[str]] = None,
    key_separator: str = "_",
    timeout_ms: Optional[int] = None,
    headers: Optional[Dict[str, bytes]] = None
) -> int
```

**参数说明**：
- `table`: 要发送的 `pyarrow.Table` 数据
- `topic`: Kafka 主题名称
- `key_cols`: 用于生成 Kafka key 的列名列表
- `key_separator`: key 列值之间的分隔符（默认：`"_"`）
- `timeout_ms`: 入队超时（毫秒），`None` 表示无限等待
- `headers`: Kafka 消息头（字典格式：`{name: bytes_value}`）

**返回值**：成功入队的行数

#### flush 方法

```python
def flush(self, timeout_ms: Optional[int] = None) -> None
```

等待所有已入队的消息被 Kafka broker 确认。

#### close 方法

```python
def close(self) -> None
```

关闭 Sink，隐式调用 `flush(timeout_ms=30000)`。

#### stats 方法

```python
def stats(self) -> SinkStats
```

获取运行时的统计信息。

### SinkStats 类

```python
class SinkStats:
    enqueued_total: int      # 总入队行数
    flush_count: int         # flush 调用次数
    sr_cache_hits: int       # Schema Registry 缓存命中
    sr_cache_misses: int     # Schema Registry 缓存未命中
    
    def sr_total_lookups(self) -> int: ...
    def sr_hit_rate(self) -> float: ...
```

### 工具函数

#### create_topic_if_not_exists

```python
def create_topic_if_not_exists(
    bootstrap_servers: str,
    topic: str,
    num_partitions: int,
    replication_factor: int,
    timeout_ms: int
) -> bool
```

创建主题（如果不存在）。返回 `True` 表示新创建，`False` 表示已存在。

## ⚙️ 配置参数详解

### Kafka 连接参数

| 参数 | 默认值 | 说明 | 生产建议 |
|------|-------|------|---------|
| `kafka_servers` | - | Kafka 集群地址，逗号分隔 | 至少3个broker，不同机房 |
| `max_in_flight` | 1000 | 每连接最大在途请求数 | 幂等模式：≤5，普通模式：100-1000 |
| `request_timeout_ms` | 30000 | 请求超时（毫秒） | 根据网络延迟调整 |

### 可靠性参数

| 参数 | 默认值 | 说明 | 场景 |
|------|-------|------|------|
| `enable_idempotence` | False | 启用幂等生产 | 金融交易、精确一次语义 |
| `acks` | "1" | 确认级别 | `"0"`: 最快, `"1"`: 平衡, `"all"`: 最可靠 |
| `retries` | None | 重试次数 | `None`: 无限重试（推荐） |
| `retry_backoff_ms` | 100 | 重试间隔 | 100-1000ms，根据集群负载调整 |

### 性能参数

| 参数 | 默认值 | 说明 | 调优建议 |
|------|-------|------|---------|
| `linger_ms` | 20 | 批量等待时间 | 低延迟：0-5，高吞吐：20-100 |
| `batch_size` | 65536 | 批次大小（字节） | 根据消息大小调整，通常64K-1M |
| `compression_type` | "none" | 压缩算法 | `lz4`（推荐）、`snappy`、`zstd` |

### Schema Registry 参数

| 参数 | 默认值 | 说明 | 兼容性 |
|------|-------|------|--------|
| `subject_name_strategy` | "topic_name" | 主题命名策略 | `"topic_name"`: Materialize 兼容 |

## 🚨 错误处理

### 异常层次结构

```dev/null/exceptions.md#L1-15
ArrowKafkaError (RuntimeError)
├── SchemaRegistryError    # Schema Registry 错误
├── SerializationError     # 序列化错误
├── EnqueueError           # 入队错误（exc.args[1] = 已入队行数）
├── FlushTimeoutError      # Flush 超时
├── UnsupportedTypeError   # 不支持的数据类型
├── ConfigError           # 配置错误
└── AdminError            # 管理操作错误
```

### 错误处理示例

```python
# error_handling.py
from arrow_kafka_pyo3 import (
    ArrowKafkaSink,
    SchemaRegistryError,
    EnqueueError,
    FlushTimeoutError,
    SerializationError
)

sink = ArrowKafkaSink(...)

try:
    # 尝试发送数据
    rows_sent = sink.consume_arrow(table, topic="data", timeout_ms=1000)
    
    # 等待确认
    sink.flush(timeout_ms=5000)
    
except SchemaRegistryError as e:
    # Schema Registry 错误（网络问题、模式不兼容）
    print(f"Schema Registry 错误: {e}")
    # 检查网络连接、Schema 兼容性
    
except EnqueueError as e:
    # 入队错误（队列满、超时）
    rows_done = e.args[1]  # 获取已入队的行数
    print(f"部分成功: {rows_done} 行已入队")
    
    # 可以重试剩余行
    remaining_table = table.slice(rows_done)
    if remaining_table.num_rows > 0:
        sink.consume_arrow(remaining_table, topic="data")
    
except FlushTimeoutError as e:
    # Flush 超时
    print(f"Flush 超时: {e}")
    
    # 可以选择：1) 重试 flush，2) 记录警告继续，3) 抛出异常
    sink.flush(timeout_ms=30000)  # 使用更长超时重试
    
except SerializationError as e:
    # 序列化错误（数据类型不支持、空值问题）
    print(f"序列化错误: {e}")
    # 检查数据质量、类型转换
    
except Exception as e:
    # 其他未知错误
    print(f"未知错误: {e}")
    sink.close()
    raise
```

### 调试技巧

```python
# debug_techniques.py
import logging
import time

# 1. 启用详细日志
logging.basicConfig(level=logging.DEBUG)

# 2. 监控统计信息
def monitor_sink(sink: ArrowKafkaSink, interval_sec: int = 5):
    while True:
        stats = sink.stats()
        print(f"[监控] 入队: {stats.enqueued_total}, "
              f"缓存命中率: {stats.sr_hit_rate():.1%}, "
              f"Flush次数: {stats.flush_count}")
        time.sleep(interval_sec)

# 3. 性能分析
def benchmark_sink(sink: ArrowKafkaSink, table: pa.Table, iterations: int = 100):
    import time
    
    total_rows = 0
    total_time = 0
    
    for i in range(iterations):
        start_time = time.time()
        rows = sink.consume_arrow(table, topic="benchmark")
        sink.flush()
        end_time = time.time()
        
        total_rows += rows
        total_time += (end_time - start_time)
        
        if i % 10 == 0:
            print(f"迭代 {i}: {rows/ (end_time-start_time):.0f} 行/秒")
    
    print(f"平均吞吐量: {total_rows/total_time:.0f} 行/秒")
```

## 🚀 性能调优

### 性能配置预设

#### 预设1：低延迟模式（实时交易）

```python
low_latency_sink = ArrowKafkaSink(
    linger_ms=0,           # 无批量等待
    batch_size=16384,      # 小批次
    compression_type="none", # 无压缩（CPU 开销最小）
    max_in_flight=5,       # 小窗口
    enable_idempotence=True, # 精确一次
    acks="1",              # Leader 确认（平衡延迟和可靠性）
    retries=3,             # 有限重试
    retry_backoff_ms=50,   # 快速重试
    request_timeout_ms=5000 # 激进超时
)
```

**性能特征**：
- 延迟：1-10ms
- 吞吐：10-100 MB/s
- 适用：实时交易、交互式应用

#### 预设2：高吞吐模式（ETL 管道）

```python
high_throughput_sink = ArrowKafkaSink(
    linger_ms=100,         # 100ms 批量等待
    batch_size=1048576,    # 1MB 批次
    compression_type="lz4", # LZ4 压缩（速度与压缩比平衡）
    max_in_flight=1000,    # 大窗口
    enable_idempotence=False, # 禁用幂等（更高吞吐）
    acks="0",              # 无确认（最快）
    retries=None,          # 无限重试
    retry_backoff_ms=100,  # 标准重试间隔
    request_timeout_ms=30000 # 保守超时
)
```

**性能特征**：
- 延迟：20-100ms
- 吞吐：500 MB/s - 1 GB/s+
- 适用：批处理、日志聚合、数据仓库 ETL

### 压缩算法选择

| 算法 | 压缩比 | 速度 | CPU 开销 | 推荐场景 |
|------|-------|------|---------|---------|
| `none` | 1.0x | 🚀 最快 | 最低 | 低延迟、CPU 敏感 |
| `snappy` | 2.0-2.5x | ⚡ 快 | 低 | 通用场景 |
| `lz4` | 2.5-3.0x | ⚡ 很快 | 中 | 高吞吐流式 |
| `zstd` | 3.0-5.0x | 🐢 中等 | 高 | 带宽受限 |
| `gzip` | 3.0-5.0x | 🐌 慢 | 高 | 归档存储 |

### 监控指标解读

```python
# monitoring_indicators.py
stats = sink.stats()

# 关键指标
print(f"1. 缓存命中率: {stats.sr_hit_rate():.1%}")
#   > 99%: 优秀，Schema 重用良好
#   90-99%: 良好，可能有多个 Schema
#   < 90%: 需要优化，Schema 变化频繁

print(f"2. Flush 频率: {stats.flush_count}")
#   与 enqueued_total 比较，判断是否及时 flush

print(f"3. 行入队速率: 需要计算差值")
#   计算两个时间点的 enqueued_total 差值

# 告警规则
def check_health(stats: SinkStats, prev_stats: SinkStats) -> List[str]:
    warnings = []
    
    # 1. 缓存命中率低
    if stats.sr_hit_rate() < 0.9:
        warnings.append(f"Schema Registry 缓存命中率低: {stats.sr_hit_rate():.1%}")
    
    # 2. 没有 flush 调用（可能数据未确认）
    if stats.flush_count == prev_stats.flush_count:
        warnings.append("长时间未调用 flush()，消息可能未确认")
    
    # 3. 入队停滞
    if stats.enqueued_total == prev_stats.enqueued_total:
        warnings.append("数据入队停滞，可能生产者阻塞")
    
    return warnings
```

## 💡 最佳实践

### 1. 数据预处理

```python
# data_preparation.py
import pyarrow as pa
import pyarrow.compute as pc

def prepare_financial_data(df):
    """准备金融数据，确保类型兼容"""
    table = pa.table(df)
    
    # 1. 处理时间戳
    if "timestamp" in table.column_names:
        # 确保时间戳为合适的精度
        col = table.column("timestamp")
        if pa.types.is_string(col.type):
            # 字符串转时间戳
            table = table.set_column(
                table.column_names.index("timestamp"),
                "timestamp",
                pc.strptime(col, format="%Y-%m-%d %H:%M:%S", unit="millisecond")
            )
    
    # 2. 处理 Decimal
    if "amount" in table.column_names:
        # 确保 Decimal 精度
        col = table.column("amount")
        if not pa.types.is_decimal(col.type):
            table = table.set_column(
                table.column_names.index("amount"),
                "amount",
                pc.cast(col, pa.decimal128(20, 2))
            )
    
    # 3. 处理 NULL 值
    for i, (name, col) in enumerate(zip(table.column_names, table.columns)):
        null_count = col.null_count
        if null_count > 0 and not col.type.equals(pa.null()):
            print(f"警告: 列 '{name}' 有 {null_count} 个 NULL 值")
    
    return table
```

### 2. 连接池管理

```python
# connection_pool.py
from typing import List
import threading
from concurrent.futures import ThreadPoolExecutor

class SinkPool:
    def __init__(self, size: int, **sink_config):
        self.sinks = [ArrowKafkaSink(**sink_config) for _ in range(size)]
        self.lock = threading.Lock()
        self.index = 0
        
    def get_sink(self) -> ArrowKafkaSink:
        with self.lock:
            sink = self.sinks[self.index]
            self.index = (self.index + 1) % len(self.sinks)
            return sink
    
    def produce_parallel(self, tables: List[pa.Table], topic: str):
        """并行发送多个表"""
        with ThreadPoolExecutor(max_workers=len(self.sinks)) as executor:
            futures = []
            for i, table in enumerate(tables):
                sink = self.get_sink()
                future = executor.submit(
                    lambda s, t: s.consume_arrow(t, topic=topic),
                    sink, table
                )
                futures.append(future)
            
            # 等待所有完成
            for future in futures:
                future.result()
            
            # 所有 Sink flush
            for sink in self.sinks:
                sink.flush()
    
    def close_all(self):
        for sink in self.sinks:
            sink.close()
```

### 3. 优雅关闭

```python
# graceful_shutdown.py
import signal
import sys
from contextlib import contextmanager

class GracefulShutdown:
    def __init__(self, sinks: List[ArrowKafkaSink]):
        self.sinks = sinks
        self.shutting_down = False
        signal.signal(signal.SIGINT, self.handle_signal)
        signal.signal(signal.SIGTERM, self.handle_signal)
    
    def handle_signal(self, signum, frame):
        if self.shutting_down:
            sys.exit(1)  # 强制退出
        
        self.shutting_down = True
        print("\n收到关闭信号，正在优雅关闭...")
        
        for sink in self.sinks:
            try:
                sink.close()  # 会隐式调用 flush
            except Exception as e:
                print(f"关闭 Sink 时出错: {e}")
        
        print("所有 Sink 已关闭")
        sys.exit(0)
    
    @contextmanager
    def production_context(self):
        """上下文管理器，确保资源清理"""
        try:
            yield
        finally:
            if not self.shutting_down:
                for sink in self.sinks:
                    sink.close()

# 使用示例
sink1 = ArrowKafkaSink(...)
sink2 = ArrowKafkaSink(...)

shutdown_handler = GracefulShutdown([sink1, sink2])

with shutdown_handler.production_context():
    # 正常生产循环
    while not shutdown_handler.shutting_down:
        data = get_next_batch()
        sink1.consume_arrow(data, topic="events")
        sink1.flush()
```

### 4. Schema 管理策略

```python
# schema_management.py
import json
from typing import Dict, Any

class SchemaManager:
    def __init__(self, schema_registry_url: str):
        self.schema_registry_url = schema_registry_url
        self.schema_cache: Dict[str, Dict] = {}
    
    def get_avro_schema(self, arrow_schema: pa.Schema) -> Dict[str, Any]:
        """为 Arrow Schema 生成优化的 Avro Schema"""
        schema_hash = str(hash(str(arrow_schema)))
        
        if schema_hash in self.schema_cache:
            return self.schema_cache[schema_hash]
        
        # 生成 Avro Schema（简化示例）
        avro_schema = {
            "type": "record",
            "name": "arrow_schema",
            "fields": []
        }
        
        for field in arrow_schema:
            avro_field = {
                "name": field.name,
                "type": self._arrow_to_avro_type(field.type)
            }
            
            if field.nullable:
                avro_field["type"] = ["null", avro_field["type"]]
                avro_field["default"] = None
            
            avro_schema["fields"].append(avro_field)
        
        self.schema_cache[schema_hash] = avro_schema
        return avro_schema
    
    def _arrow_to_avro_type(self, arrow_type: pa.DataType) -> Any:
        """Arrow 类型转 Avro 类型"""
        type_map = {
            pa.string(): "string",
            pa.int64(): "long",
            pa.int32(): "int",
            pa.float64(): "double",
            pa.float32(): "float",
            pa.bool_(): "boolean",
            pa.date32(): {"type": "int", "logicalType": "date"},
            pa.timestamp("ms"): {"type": "long", "logicalType": "timestamp-millis"},
            pa.decimal128(20, 2): {
                "type": "bytes",
                "logicalType": "decimal",
                "precision": 20,
                "scale": 2
            }
        }
        
        return type_map.get(arrow_type, "string")  # 默认回退到 string
```

## ❓ 常见问题

### Q1: 如何选择合适的 `linger_ms` 和 `batch_size`？

**A**: 遵循以下原则：
1. **计算目标延迟**：如果你的应用要求 < 10ms 延迟，使用 `linger_ms=0`
2. **估算消息大小**：`batch_size` 应该是典型消息大小的 10-100 倍
3. **测试验证**：使用实际数据进行基准测试

```python
# 自动调优函数
def auto_tune_config(avg_message_size: int, target_latency_ms: int):
    if target_latency_ms < 10:
        return {"linger_ms": 0, "batch_size": 16384}
    elif target_latency_ms < 50:
        return {"linger_ms": 5, "batch_size": 65536}
    else:
        return {"linger_ms": 20, "batch_size": 262144}
```

### Q2: 为什么需要调用 `flush()`？

**A**: `consume_arrow()` 只保证数据被 **入队** 到 librdkafka 的发送缓冲区，不保证被 Kafka broker 确认。调用 `flush()` 是确保数据被持久化到 Kafka 的关键步骤。

**最佳实践**：
- 定期调用 `flush()`（例如每 1000 条消息或每秒一次）
- 在应用关闭前调用 `close()`（会隐式调用 `flush()`）
- 使用 `flush(timeout_ms)` 设置合理的超时

### Q3: 如何处理 Schema 演化？

**A**: 参考 [Schema 演化指南](docs/schema_evolution.md)，关键原则：
1. **只添加可空字段**（带默认值）
2. **不要删除字段**
3. **不要修改字段类型**
4. **使用别名处理重命名**

### Q4: 内存使用过高怎么办？

**解决方案**：
1. 减小 `batch_size`
2. 更频繁地调用 `flush()` 释放缓冲区
3. 使用更高效的压缩算法（如 `lz4`）
4. 监控 `SinkStats.enqueued_total` 的增长速度

### Q5: 如何监控生产状态？

**监控方案**：
```python
def monitor_production():
    prev_stats = sink.stats()
    
    while True:
        time.sleep(5)  # 5秒间隔
        current_stats = sink.stats()
        
        # 计算速率
        rows_per_sec = (current_stats.enqueued_total - prev_stats.enqueued_total) / 5
        
        print(f"生产速率: {rows_per_sec:.0f} 行/秒")
        print(f"缓存命中率: {current_stats.sr_hit_rate():.1%}")
        print(f"未 flush 行数: {current_stats.enqueued_total}")
        
        prev_stats = current_stats
```

### Q6: 支持哪些 Arrow 数据类型？

**完整列表**：
- ✅ **字符串**: `Utf8`, `LargeUtf8`
- ✅ **整数**: `Int8`, `Int16`, `Int32`, `Int64`, `UInt8`, `UInt16`, `UInt32`, `UInt64`
- ✅ **浮点**: `Float32`, `Float64`
- ✅ **布尔**: `Boolean`
- ✅ **时间**: `Date32`, `Date64`, `Timestamp`（所有单位）
- ✅ **二进制**: `Binary`, `LargeBinary`, `FixedSizeBinary`
- ✅ **十进制**: `Decimal128`
- ❌ **复杂类型**: `List`, `Struct`, `Map`（暂不支持）

## 🎯 总结

Arrow-Kafka-Pyo3 是一个为生产环境设计的高性能 Kafka 数据下沉库。通过遵循本指南的最佳实践，你可以：

1. **快速开始**：使用提供的示例代码快速集成
2. **优化性能**：根据应用场景选择合适的配置预设
3. **确保可靠性**：合理使用错误处理和监控机制
4. **维护可扩展性**：采用推荐的架构模式

### 下一步行动

1. **运行示例**：尝试 [快速开始](#快速开始) 中的代码示例
2. **性能测试**：使用你的数据进行基准测试
3. **生产部署**：参考配置预设调整参数
4. **监控集成**：将 `SinkStats` 集成到你的监控系统

### 获取帮助

- **GitHub Issues**: 报告 bug 或请求功能
- **文档改进**: 提交文档更新的 Pull Request
- **社区支持**: 加入相关技术社区讨论

---

**最后更新**: 2024年1月
**版本**: 1.0.0
**作者**: Arrow-Kafka-Pyo3 团队

> 提示：本指南基于 Arrow-Kafka-Pyo3 v0.1.0+ 版本编写，不同版本可能存在差异。