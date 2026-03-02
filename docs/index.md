# Arrow-Kafka-Pyo3 Documentation

[English](#english-documentation) | [中文文档](#中文文档)

---

## English Documentation

### Overview

Arrow-Kafka-Pyo3 is a high-performance Kafka sink library with Arrow zero-copy support, designed for financial data, real-time streaming, and batch processing scenarios.

### Quick Links

| Documentation | Description |
|---------------|-------------|
| [📖 README](../README.md) | Project introduction, installation, and basic usage |
| [🚀 Getting Started](#) | Quick start guide for new users |
| [🔧 User Guide](#) | Complete user manual |

### Core Documentation

#### 1. User Guides
- **[Getting Started](getting_started.md)** - Installation and first steps
- **[User Guide](user_guide.md)** - Complete user manual
- **[API Reference](../crates/arrow-kafka-pyo3/arrow_kafka_pyo3.pyi)** - Python API documentation

#### 2. Configuration & Tuning
- **[Performance Tuning](tuning.md)** - Configuration presets and optimization guide
- **[Schema Evolution](schema_evolution.md)** - Schema compatibility and Materialize integration
- **[Deployment Guide](deployment.md)** - Production deployment guide

#### 3. Reference Documentation
- **[Error Handling](error_handling.md)** - Exception hierarchy and handling
- **[Monitoring](monitoring.md)** - Metrics, logging, and observability
- **[Best Practices](best_practices.md)** - Production recommendations

### Advanced Topics

#### 4. Architecture & Design
- **[Architecture Overview](architecture.md)** - System design and components
- **[Data Flow](data_flow.md)** - Arrow to Kafka data pipeline
- **[Performance Characteristics](performance.md)** - Throughput and latency characteristics

#### 5. Integration Guides
- **[Materialize Integration](materialize_integration.md)** - Materialize compatibility guide
- **[Redpanda Configuration](redpanda.md)** - Redpanda-specific configuration
- **[Schema Registry](schema_registry.md)** - Schema Registry integration details

### Development Resources

#### 6. For Developers
- **[Contributing Guide](../CONTRIBUTING.md)** - How to contribute to the project
- **[Development Setup](development.md)** - Setting up development environment
- **[Testing Guide](testing.md)** - Running tests and benchmarks

#### 7. Project Information
- **[Changelog](../CHANGELOG.md)** - Version history and changes
- **[Roadmap](roadmap.md)** - Future development plans
- **[FAQ](faq.md)** - Frequently asked questions

---

## 中文文档

### 概述

Arrow-Kafka-Pyo3 是一个高性能的 Kafka 数据下沉库，支持 Arrow 零拷贝传输，专为金融数据、实时流处理和批处理场景设计。

### 快速链接

| 文档 | 描述 |
|------|------|
| [📖 项目介绍](../README.zh.md) | 项目介绍、安装和基础使用 |
| [🚀 快速开始](zh/getting_started.md) | 新用户快速入门指南 |
| [🔧 用户指南](zh/user_guide.md) | 完整的用户手册 |

### 核心文档

#### 1. 用户指南
- **[快速开始](zh/getting_started.md)** - 安装和第一步
- **[用户指南](zh/user_guide.md)** - 完整的用户手册
- **[API 参考](../crates/arrow-kafka-pyo3/arrow_kafka_pyo3.pyi)** - Python API 文档

#### 2. 配置与调优
- **[性能调优](tuning.md)** - 配置预设和优化指南
- **[Schema 演化](schema_evolution.md)** - Schema 兼容性和 Materialize 集成
- **[部署指南](zh/deployment.md)** - 生产环境部署指南

#### 3. 参考文档
- **[错误处理](zh/error_handling.md)** - 异常体系和处理方法
- **[监控与运维](zh/monitoring.md)** - 指标、日志和可观测性
- **[最佳实践](zh/best_practices.md)** - 生产环境建议

### 高级主题

#### 4. 架构与设计
- **[架构概述](zh/architecture.md)** - 系统设计和组件
- **[数据流](zh/data_flow.md)** - Arrow 到 Kafka 的数据管道
- **[性能特性](zh/performance.md)** - 吞吐量和延迟特性

#### 5. 集成指南
- **[Materialize 集成](zh/materialize_integration.md)** - Materialize 兼容性指南
- **[Redpanda 配置](zh/redpanda.md)** - Redpanda 特定配置
- **[Schema Registry](zh/schema_registry.md)** - Schema Registry 集成详情

### 开发资源

#### 6. 开发者文档
- **[贡献指南](../CONTRIBUTING.md)** - 如何参与项目开发
- **[开发环境设置](zh/development.md)** - 设置开发环境
- **[测试指南](zh/testing.md)** - 运行测试和基准测试

#### 7. 项目信息
- **[更新日志](../CHANGELOG.md)** - 版本历史记录和变更
- **[路线图](zh/roadmap.md)** - 未来发展计划
- **[常见问题](zh/faq.md)** - 常见问题解答

---

## Document Status

| Document | Status | Last Updated |
|----------|--------|--------------|
| User Guide (中文) | ✅ Complete | 2024-01 |
| Deployment Guide (中文) | ✅ Complete | 2024-01 |
| Performance Tuning | ✅ Complete | 2024-01 |
| Schema Evolution | ✅ Complete | 2024-01 |
| Getting Started | 🔄 In Progress | - |
| API Reference | 🔄 In Progress | - |
| Architecture Overview | ⏳ Planned | - |

## Contributing to Documentation

We welcome contributions to improve our documentation! Please see our [Contributing Guide](../CONTRIBUTING.md) for details on how to submit documentation improvements.

## Need Help?

- **GitHub Issues**: [Report documentation issues](https://github.com/your-org/arrow-kafka/issues)
- **Discussions**: [Join the conversation](https://github.com/your-org/arrow-kafka/discussions)
- **Email**: docs@arrow-kafka.example.com

---

*Last Updated: January 2024*  
*Version: 1.0.0*