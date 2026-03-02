# Arrow-Kafka-Pyo3 快速开始指南

<a href="../../README.md">English</a> | <a href="user-guide.md">用户指南</a> | <a href="api-reference.md">API 参考</a>

## 🎯 项目简介

Arrow-Kafka-Pyo3 是一个高性能的 Kafka 数据下沉库，支持 Arrow 零拷贝传输。它专为金融数据、实时流处理和批处理场景设计。

### 核心特性

- **零拷贝传输**：通过 Arrow FFI 直接从 `pyarrow.Table` 到 Avro，无需内存复制
- **完整类型支持**：覆盖所有金融数据 Arrow 类型（Date32、Timestamp、Decimal128 等）
- **结构化错误处理**：7 个专用异常类，清晰的错误上下文
- **生产就绪**：支持幂等生产、精确一次语义、可配置的重试策略
- **Materialize 兼容**：使用 Confluent 线格式，直接兼容 Materialize 数据源

## 📦 安装

### 从 PyPI 安装（推荐）

```bash
pip install arrow-kafka-pyo3
```

### 从源码安装（开发环境）

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

### 环境要求

- **Python**: 3.8 或更高版本
- **pyarrow**: 8.0.0 或更高版本
- **Kafka/Redpanda**: 用于数据生产（测试可以使用 Docker）

## 🚀 5分钟快速开始

### 1. 准备测试环境（可选）

```bash
# 启动 Redpanda 和 Schema Registry（使用 Docker）
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

### 2. 基础使用示例

```python
# basic_example.py
import pyarrow as pa
import pandas as pd
from arrow_kafka_pyo3 import ArrowKafkaSink

# 创建 Sink 实例
sink = ArrowKafkaSink(
    kafka_servers="localhost:9092",
    schema_registry_url="http://localhost:8081",
)

# 准备测试数据
data = {
    "symbol": ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"],
    "price": [189.3, 2750.5, 342.8, 155.2, 248.8],
    "volume": [1000, 500, 1200, 800, 1500],
    "timestamp": pd.date_range("2024-01-01 10:00:00", periods=5, freq="1s")
}

table = pa.table(data)

# 发送数据到 Kafka
try:
    rows_sent = sink.consume_arrow(
        table=table,
        topic="stock_quotes",
        key_cols=["symbol"],  # 使用 symbol 作为 Kafka key
        timeout_ms=5000       # 5秒超时
    )
    print(f"✅ 成功发送 {rows_sent} 行数据")
    
    # 等待所有消息被 Kafka 确认
    sink.flush(timeout_ms=10000)
    print("✅ 所有消息已被 Kafka broker 确认")
    
except Exception as e:
    print(f"❌ 发送失败: {e}")

# 关闭连接
sink.close()
print("✅ 连接已关闭")
```

### 3. 运行示例

```bash
python basic_example.py
```

## ⚙️ 基础配置

### 最小配置

```python
sink = ArrowKafkaSink(
    # 必需参数
    kafka_servers="localhost:9092",         # Kafka 集群地址
    schema_registry_url="http://localhost:8081",  # Schema Registry 地址
    
    # 可选参数
    linger_ms=20,           # 批量等待时间（毫秒）
    batch_size=65536,       # 批次大小（字节）
    compression_type="lz4", # 压缩算法：none, gzip, snappy, lz4, zstd
)
```

### 配置预设

#### 低延迟模式（实时交易）

```python
low_latency_sink = ArrowKafkaSink(
    linger_ms=0,           # 无批量等待
    batch_size=16384,      # 小批次
    compression_type="none", # 无压缩
    enable_idempotence=True, # 精确一次语义
    acks="1",              # Leader 确认
    max_in_flight=5,       # 小窗口（幂等模式要求）
)
```

#### 高吞吐模式（批处理）

```python
high_throughput_sink = ArrowKafkaSink(
    linger_ms=100,         # 100ms 批量等待
    batch_size=1048576,    # 1MB 批次
    compression_type="lz4", # LZ4 压缩
    enable_idempotence=False, # 禁用幂等（更高吞吐）
    acks="0",              # 无确认（最快）
    max_in_flight=1000,    # 大窗口
)
```

## 📊 基础监控

```python
from arrow_kafka_pyo3 import ArrowKafkaSink

sink = ArrowKafkaSink(
    kafka_servers="localhost:9092",
    schema_registry_url="http://localhost:8081",
)

# 获取运行时统计信息
stats = sink.stats()
print(f"总入队行数: {stats.enqueued_total}")
print(f"Schema Registry 缓存命中率: {stats.sr_hit_rate():.1%}")
print(f"缓存命中: {stats.sr_cache_hits}, 缓存未命中: {stats.sr_cache_misses}")

sink.close()
```

## 🚨 基础错误处理

```python
from arrow_kafka_pyo3 import (
    ArrowKafkaSink,
    SchemaRegistryError,
    EnqueueError,
    FlushTimeoutError
)

sink = ArrowKafkaSink(...)

try:
    rows_sent = sink.consume_arrow(table, topic="data", timeout_ms=1000)
    sink.flush(timeout_ms=5000)
    
except SchemaRegistryError as e:
    print(f"Schema Registry 错误: {e}")
    # 检查网络连接或 Schema 兼容性
    
except EnqueueError as e:
    rows_done = e.args[1]  # 获取已入队的行数
    print(f"部分成功: {rows_done} 行已入队")
    # 可以重试剩余行
    
except FlushTimeoutError as e:
    print(f"Flush 超时: {e}")
    # 可以重试 flush 或接受潜在的数据丢失
```

## 🧪 测试安装

创建一个简单的测试脚本验证安装是否成功：

```python
# test_installation.py
import pyarrow as pa
from arrow_kafka_pyo3 import ArrowKafkaSink, __version__

print(f"✅ Arrow-Kafka-Pyo3 版本: {__version__}")

# 测试基础功能
try:
    sink = ArrowKafkaSink(
        kafka_servers="localhost:9092",
        schema_registry_url="http://localhost:8081",
    )
    print("✅ Sink 创建成功")
    
    table = pa.table({"test": [1, 2, 3]})
    print("✅ Arrow 表格创建成功")
    
    sink.close()
    print("✅ 所有基础测试通过！")
    
except Exception as e:
    print(f"❌ 测试失败: {e}")
```

## 📚 下一步

### 学习更多

1. **阅读完整文档**：
   - **[用户指南](user-guide.md)** - 详细的使用说明和最佳实践
   - **[API 参考](api-reference.md)** - 所有方法、参数、异常说明
   - **[Schema 演化指南](schema-evolution.md)** - Schema 兼容性规则

2. **查看示例代码**：
   ```bash
   # 查看项目中的示例
   find examples/ -name "*.py" -type f
   ```

## ❓ 快速问答

### Q: 遇到 "Connection refused" 错误怎么办？
**A**: 确保 Kafka/Redpanda 正在运行并且可以从你的应用程序访问。

### Q: 如何选择合适的压缩算法？
**A**:
- **低延迟**：`none` 或 `snappy`
- **通用场景**：`lz4`（速度和压缩比平衡）
- **带宽受限**：`zstd` 或 `gzip`

### Q: 为什么需要调用 `flush()`？
**A**: `consume_arrow()` 只保证数据被**入队**到发送缓冲区，`flush()` 确保数据被 Kafka broker **确认**。

## 📞 获取帮助

- **GitHub Issues**: [报告问题或请求功能](https://github.com/your-org/arrow-kafka/issues)
- **文档问题**: 提交文档改进的 Pull Request

---

*最后更新: 2024年1月*  
*版本: 1.0.0*