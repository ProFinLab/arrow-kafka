# Arrow-Kafka-Pyo3 基础使用示例
# 这个示例展示了如何使用 ArrowKafkaSink 将数据发送到 Kafka

import pandas as pd
import pyarrow as pa
from arrow_kafka_pyo3 import ArrowKafkaSink


def create_sample_data():
    """创建示例数据"""
    data = {
        "symbol": ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"],
        "price": [189.3, 2750.5, 342.8, 155.2, 248.8],
        "volume": [1000, 500, 1200, 800, 1500],
        "timestamp": pd.date_range("2024-01-01 10:00:00", periods=5, freq="1s"),
    }
    return pa.table(data)


def basic_configuration():
    """基础配置示例"""
    print("=== 基础配置示例 ===")

    # 创建 Sink 实例（使用默认配置）
    sink = ArrowKafkaSink(
        kafka_servers="localhost:9092",
        schema_registry_url="http://localhost:8081",
    )

    # 获取统计信息
    stats = sink.stats()
    print(f"初始统计: 入队行数={stats.enqueued_total}, Flush次数={stats.flush_count}")

    return sink


def production_configuration():
    """生产环境配置示例"""
    print("\n=== 生产环境配置示例 ===")

    # 创建 Sink 实例（生产环境配置）
    sink = ArrowKafkaSink(
        kafka_servers="localhost:9092",
        schema_registry_url="http://localhost:8081",
        # 可靠性配置
        enable_idempotence=True,  # 启用幂等生产（精确一次）
        acks="all",  # 等待所有副本确认
        retries=10,  # 重试次数
        retry_backoff_ms=100,  # 重试间隔
        # 性能配置
        linger_ms=20,  # 20ms 批量等待
        batch_size=65536,  # 64KB 批次大小
        compression_type="lz4",  # LZ4 压缩
        max_in_flight=5,  # 幂等模式要求 ≤5
        # Schema Registry 配置
        subject_name_strategy="topic_name",  # Materialize 兼容
    )

    return sink


def send_data_with_error_handling(sink, table, topic_name):
    """发送数据并处理错误"""
    print(f"\n=== 发送数据到主题 '{topic_name}' ===")

    try:
        # 发送数据到 Kafka
        rows_sent = sink.consume_arrow(
            table=table,
            topic=topic_name,
            key_cols=["symbol"],  # 使用 symbol 列作为 Kafka key
            key_separator="_",  # key 分隔符
            timeout_ms=5000,  # 5秒超时
        )

        print(f"✅ 成功发送 {rows_sent} 行数据")

        # 等待所有消息被 Kafka 确认
        sink.flush(timeout_ms=10000)
        print("✅ 所有消息已被 Kafka broker 确认")

        return True

    except Exception as e:
        print(f"❌ 发送失败: {e}")
        return False


def demonstrate_advanced_features():
    """展示高级功能"""
    print("\n=== 高级功能示例 ===")

    sink = ArrowKafkaSink(
        kafka_servers="localhost:9092",
        schema_registry_url="http://localhost:8081",
    )

    # 1. 消息头支持
    headers = {
        "trace_id": b"abc-123-xyz",
        "source": b"example_script",
        "version": b"v1.0",
    }

    # 2. 创建测试数据
    table = pa.table(
        {
            "user_id": [1, 2, 3, 4, 5],
            "action": ["login", "purchase", "logout", "view", "search"],
            "timestamp": pd.date_range("2024-01-01 12:00:00", periods=5, freq="30s"),
        }
    )

    try:
        # 发送带 headers 的数据
        rows = sink.consume_arrow(table=table, topic="user_events", headers=headers)
        print(f"✅ 发送带 headers 的数据: {rows} 行")

        sink.flush()

    except Exception as e:
        print(f"❌ 发送失败: {e}")

    finally:
        sink.close()


def main():
    """主函数"""
    print("Arrow-Kafka-Pyo3 基础使用示例")
    print("=" * 50)

    # 创建示例数据
    table = create_sample_data()
    print(f"✅ 创建示例数据: {table.num_rows} 行, {table.num_columns} 列")
    print(f"   列名: {table.column_names}")

    # 示例 1: 基础配置
    sink1 = basic_configuration()
    success1 = send_data_with_error_handling(sink1, table, "stock_quotes_basic")
    sink1.close()

    # 示例 2: 生产环境配置
    sink2 = production_configuration()
    success2 = send_data_with_error_handling(sink2, table, "stock_quotes_prod")

    # 展示统计信息
    stats = sink2.stats()
    print("\n📊 最终统计信息:")
    print(f"  总入队行数: {stats.enqueued_total}")
    print(f"  Flush 调用次数: {stats.flush_count}")
    print(f"  Schema Registry 缓存命中率: {stats.sr_hit_rate():.1%}")
    print(f"  缓存命中: {stats.sr_cache_hits}, 缓存未命中: {stats.sr_cache_misses}")

    sink2.close()

    # 示例 3: 高级功能
    demonstrate_advanced_features()

    print("\n" + "=" * 50)
    if success1 and success2:
        print("✅ 所有示例执行完成！")
    else:
        print("⚠️  部分示例执行失败，请检查 Kafka 服务是否运行")


if __name__ == "__main__":
    # 注意事项
    print("注意：运行此示例前，请确保：")
    print("1. Kafka/Redpanda 服务正在运行")
    print("2. Schema Registry 服务正在运行")
    print("3. 可以访问 localhost:9092 和 localhost:8081")
    print("-" * 50)

    main()
