# Arrow-Kafka-Pyo3

<a href="README.md">English</a> | <a href="docs/zh-CN/getting-started.md">快速开始</a> | <a href="docs/zh-CN/user-guide.md">用户指南</a>

高性能 Kafka 数据下沉库，支持 Arrow 零拷贝传输，适用于金融数据、实时流处理和批处理场景。

## 🚀 特性亮点

### ✅ 生产就绪
- **结构化错误处理**：7 个专用异常类，清晰的错误上下文
- **完整类型支持**：覆盖所有金融数据 Arrow 类型（Date32、Timestamp、Decimal128 等）
- **可靠性配置**：支持幂等生产、精确一次语义、重试策略
- **可观测性**：内置统计计数器，监控缓存命中率和吞吐量

### 🔧 核心能力
- **零拷贝**：通过 Arrow FFI 直接从 `pyarrow.Table` 到 Avro，无内存复制
- **Schema Registry 集成**：支持 Confluent/Redpanda Schema Registry，自动模式注册
- **Materialize 兼容**：使用 Confluent 线格式，直接兼容 Materialize 数据源

## 📦 安装

### 从 PyPI 安装（推荐）
```bash
pip install arrow-kafka-pyo3
```

### 从源代码安装（开发环境）
```bash
# 安装 Rust 工具链（如未安装）
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# 克隆项目
git clone https://github.com/your-org/arrow-kafka.git
cd arrow-kafka

# 构建 Python 扩展
cd crates/arrow-kafka-pyo3
maturin develop
```

## 🚀 5分钟快速开始

```python
import pyarrow as pa
from arrow_kafka_pyo3 import ArrowKafkaSink

# 创建 sink 实例
sink = ArrowKafkaSink(
    kafka_servers="localhost:9092",
    schema_registry_url="http://localhost:8081",
)

# 准备数据
table = pa.table({
    "symbol": ["AAPL", "GOOGL", "MSFT"],
    "price": [189.3, 2750.5, 342.8],
    "volume": [1000, 500, 1200]
})

# 发送数据
rows_sent = sink.consume_arrow(
    table=table,
    topic="stock_quotes",
    key_cols=["symbol"]
)

print(f"✅ 成功发送 {rows_sent} 行数据到 Kafka")

# 确保投递完成
sink.flush(timeout_ms=10000)

# 关闭连接
sink.close()
```

更多详细示例请参考 <a href="docs/zh-CN/getting-started.md">快速开始指南</a>。

## 📚 文档导航

### 快速链接
- **[快速开始](docs/zh-CN/getting-started.md)** - 安装和基础使用
- **[完整用户指南](docs/zh-CN/user-guide.md)** - 详细的使用指南和示例
- **[API 参考](docs/zh-CN/api-reference.md)** - 详细的 API 文档
- **[Schema 演化](docs/zh-CN/schema-evolution.md)** - Schema 兼容性规则
- **[常见问题](docs/zh-CN/faq.md)** - 常见问题和故障排除
- **[英文文档](README.md)** - 完整的英文文档

### 涵盖主题
- 性能调优和配置预设
- 生产环境部署和监控
- 错误处理和异常体系
- Kafka 消息头和主题管理
- Materialize 集成示例

## 🔧 高级配置示例

```python
sink = ArrowKafkaSink(
    kafka_servers="kafka1:9092,kafka2:9092,kafka3:9092",
    schema_registry_url="http://schema-registry:8081",
    
    # 可靠性配置
    enable_idempotence=True,  # 启用幂等生产
    acks="all",              # 等待所有副本确认
    
    # 性能调优
    linger_ms=20,            # 20ms 批量等待
    batch_size=65536,        # 64KB 批次大小
    compression_type="lz4",  # LZ4 压缩
    
    # Schema Registry
    subject_name_strategy="topic_name",  # Materialize 兼容
)
```

## 📊 监控

```python
stats = sink.stats()
print(f"总入队行数: {stats.enqueued_total}")
print(f"缓存命中率: {stats.sr_hit_rate():.1%}")
print(f"缓存命中: {stats.sr_cache_hits}, 未命中: {stats.sr_cache_misses}")
```

详细监控说明请参考 <a href="docs/zh-CN/user-guide.md">用户指南</a>。

## 🧪 测试

```bash
# Rust 测试
cargo test -p arrow-kafka

# Python 测试（需要先构建扩展）
cd crates/arrow-kafka-pyo3 && maturin develop
python -m pytest tests/ -v
```

## 📈 性能基准

| 场景 | 吞吐量 | 延迟 | 内存使用 |
|------|--------|------|----------|
| 低延迟模式 | 10-100 MB/s | 1-10ms | 低 |
| 高吞吐模式 | 500 MB/s+ | 20-100ms | 中等 |
| 精确一次模式 | 100-300 MB/s | 10-50ms | 低 |

## 🤝 贡献指南

欢迎提交 Issue 和 Pull Request！详见 [CONTRIBUTING.md](CONTRIBUTING.md)。

### 开发环境设置
```bash
rustup install stable
pip install -r requirements-dev.txt
pre-commit install
```

## 📄 许可证

MIT 许可证 - 查看 [LICENSE](LICENSE) 文件了解详情。

## 🙏 致谢

- [librdkafka](https://github.com/edenhill/librdkafka) - 可靠的 Kafka 客户端
- [Apache Arrow](https://arrow.apache.org/) - 零拷贝数据交换
- [Materialize](https://materialize.com/) - 实时数据仓库

## 📞 支持

- **GitHub Issues**: [报告问题或建议功能](https://github.com/your-org/arrow-kafka/issues)
- **文档问题**: 提交文档改进的 Pull Request

---

**Arrow-Kafka-Pyo3** - 为生产环境设计的高性能 Kafka 数据下沉解决方案