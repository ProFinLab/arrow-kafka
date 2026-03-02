"""
Arrow-Kafka-Pyo3 性能基准测试示例

这个脚本用于测试 Arrow-Kafka-Pyo3 在不同配置下的性能表现。
可以测试吞吐量、延迟、内存使用等关键指标。

使用方法：
    python benchmark.py --help
    python benchmark.py --scenario low_latency
    python benchmark.py --scenario high_throughput --duration 60
    python benchmark.py --scenario all --output results.json

环境要求：
    - 运行中的 Kafka/Redpanda 集群
    - Schema Registry 服务
    - Python 3.8+ 和 arrow-kafka-pyo3 库
"""

import argparse
import json
import statistics
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import List, Tuple

import pandas as pd
import pyarrow as pa
from arrow_kafka_pyo3 import ArrowKafkaSink


@dataclass
class BenchmarkResult:
    """基准测试结果"""

    scenario: str
    config_name: str
    total_rows: int
    total_bytes: int
    duration_seconds: float
    rows_per_second: float
    bytes_per_second: float
    latency_p50_ms: float
    latency_p95_ms: float
    latency_p99_ms: float
    memory_usage_mb: float
    sr_hit_rate: float
    timestamp: str

    @property
    def throughput_mbps(self) -> float:
        """计算吞吐量（MB/秒）"""
        return self.bytes_per_second / (1024 * 1024)


@dataclass
class BenchmarkConfig:
    """基准测试配置"""

    name: str
    description: str
    config: dict
    batch_sizes: List[int]
    linger_ms_values: List[int]
    num_batches: int = 10
    rows_per_batch: int = 1000


class DataGenerator:
    """数据生成器"""

    @staticmethod
    def generate_financial_data(num_rows: int) -> pa.Table:
        """生成金融数据"""
        import numpy as np

        data = {
            "symbol": [f"SYM{i:04d}" for i in range(num_rows)],
            "price": np.random.uniform(10, 1000, num_rows).astype(np.float64),
            "volume": np.random.randint(1, 10000, num_rows).astype(np.int64),
            "timestamp": pd.date_range(
                "2024-01-01 09:30:00", periods=num_rows, freq="100ms"
            ),
            "exchange": ["NYSE", "NASDAQ", "LSE", "TSE", "SSE"] * (num_rows // 5 + 1),
            "order_type": ["BUY", "SELL"] * (num_rows // 2 + 1),
            "order_id": [f"ORD{i:08d}" for i in range(num_rows)],
        }

        # 添加一些 NULL 值以模拟真实数据
        if num_rows > 10:
            for i in range(5):
                if i < len(data["price"]):
                    data["price"][i] = None
                if i + 5 < len(data["volume"]):
                    data["volume"][i + 5] = None

        return pa.table(data)

    @staticmethod
    def generate_log_data(num_rows: int) -> pa.Table:
        """生成日志数据"""
        import numpy as np

        log_levels = ["DEBUG", "INFO", "WARN", "ERROR", "FATAL"]
        services = [
            "api-gateway",
            "user-service",
            "order-service",
            "payment-service",
            "auth-service",
        ]

        data = {
            "timestamp": pd.date_range(
                "2024-01-01 00:00:00", periods=num_rows, freq="10ms"
            ),
            "level": [log_levels[i % len(log_levels)] for i in range(num_rows)],
            "service": [services[i % len(services)] for i in range(num_rows)],
            "message": [f"Log message {i} with some details" for i in range(num_rows)],
            "request_id": [f"REQ{i:010d}" for i in range(num_rows)],
            "duration_ms": np.random.exponential(100, num_rows).astype(np.float32),
            "status_code": np.random.choice([200, 201, 400, 401, 404, 500], num_rows),
            "user_agent": ["Mozilla/5.0", "Python-requests/2.31", "curl/7.88"]
            * (num_rows // 3 + 1),
        }

        return pa.table(data)

    @staticmethod
    def generate_binary_data(num_rows: int) -> pa.Table:
        """生成二进制数据"""

        # 生成一些二进制数据
        binary_data = []
        for i in range(num_rows):
            # 生成随机字节
            import random

            length = random.randint(10, 1000)
            binary_data.append(bytes([random.randint(0, 255) for _ in range(length)]))

        data = {
            "id": [i for i in range(num_rows)],
            "data": binary_data,
            "size": [len(d) for d in binary_data],
            "checksum": [hash(d) % 10000 for d in binary_data],
            "timestamp": pd.date_range(
                "2024-01-01 12:00:00", periods=num_rows, freq="50ms"
            ),
        }

        return pa.table(data)

    @staticmethod
    def get_table_size_bytes(table: pa.Table) -> int:
        """估算表格的字节大小"""
        total_bytes = 0
        for column in table.columns:
            # 估算每列的大小
            if pa.types.is_string(column.type):
                # 字符串：长度 + 内容
                total_bytes += sum(
                    len(str(val)) if val is not None else 0 for val in column
                )
            elif pa.types.is_integer(column.type):
                # 整数：8字节
                total_bytes += len(column) * 8
            elif pa.types.is_floating(column.type):
                # 浮点数：8字节
                total_bytes += len(column) * 8
            elif pa.types.is_timestamp(column.type):
                # 时间戳：8字节
                total_bytes += len(column) * 8
            elif pa.types.is_binary(column.type):
                # 二进制：长度 + 内容
                total_bytes += sum(len(val) if val is not None else 0 for val in column)
            else:
                # 其他类型：估算 16 字节
                total_bytes += len(column) * 16
        return total_bytes


class PerformanceBenchmark:
    """性能基准测试类"""

    def __init__(self, kafka_servers: str, schema_registry_url: str):
        self.kafka_servers = kafka_servers
        self.schema_registry_url = schema_registry_url

        # 预定义配置预设
        self.config_presets = {
            "low_latency": {
                "name": "低延迟模式",
                "description": "针对实时交易场景优化",
                "config": {
                    "linger_ms": 0,
                    "batch_size": 16384,
                    "compression_type": "none",
                    "max_in_flight": 5,
                    "enable_idempotence": True,
                    "acks": "1",
                    "retries": 3,
                    "retry_backoff_ms": 50,
                },
            },
            "high_throughput": {
                "name": "高吞吐模式",
                "description": "针对批处理场景优化",
                "config": {
                    "linger_ms": 100,
                    "batch_size": 1048576,
                    "compression_type": "lz4",
                    "max_in_flight": 1000,
                    "enable_idempotence": False,
                    "acks": "0",
                    "retries": None,
                    "retry_backoff_ms": 100,
                },
            },
            "reliable": {
                "name": "可靠模式",
                "description": "针对金融数据场景优化",
                "config": {
                    "linger_ms": 20,
                    "batch_size": 65536,
                    "compression_type": "snappy",
                    "max_in_flight": 5,
                    "enable_idempotence": True,
                    "acks": "all",
                    "retries": 10,
                    "retry_backoff_ms": 100,
                },
            },
            "balanced": {
                "name": "平衡模式",
                "description": "通用场景平衡配置",
                "config": {
                    "linger_ms": 20,
                    "batch_size": 65536,
                    "compression_type": "lz4",
                    "max_in_flight": 100,
                    "enable_idempotence": False,
                    "acks": "1",
                    "retries": 5,
                    "retry_backoff_ms": 100,
                },
            },
        }

        # 数据生成器
        self.data_generator = DataGenerator()

    def create_sink(self, config: dict) -> ArrowKafkaSink:
        """创建 Sink 实例"""
        base_config = {
            "kafka_servers": self.kafka_servers,
            "schema_registry_url": self.schema_registry_url,
            "subject_name_strategy": "topic_name",
            "request_timeout_ms": 30000,
        }

        # 合并配置
        full_config = {**base_config, **config}

        return ArrowKafkaSink(**full_config)

    def run_single_test(
        self,
        sink: ArrowKafkaSink,
        table: pa.Table,
        topic: str,
        num_iterations: int = 10,
    ) -> Tuple[List[float], int, int]:
        """
        运行单个测试

        Returns:
            (latencies_ms, total_rows, total_bytes)
        """
        latencies_ms = []
        total_rows = 0
        total_bytes = 0

        # 获取表格大小
        table_bytes = self.data_generator.get_table_size_bytes(table)

        for i in range(num_iterations):
            start_time = time.time()

            try:
                # 发送数据
                rows_sent = sink.consume_arrow(
                    table=table, topic=topic, timeout_ms=10000
                )

                # 等待确认
                sink.flush(timeout_ms=15000)

                end_time = time.time()
                duration_ms = (end_time - start_time) * 1000

                latencies_ms.append(duration_ms)
                total_rows += rows_sent
                total_bytes += table_bytes

                # 进度显示
                if (i + 1) % max(1, num_iterations // 10) == 0:
                    avg_latency = statistics.mean(latencies_ms) if latencies_ms else 0
                    print(
                        f"  迭代 {i + 1}/{num_iterations}: "
                        f"延迟={avg_latency:.1f}ms, "
                        f"累计行数={total_rows}"
                    )

            except Exception as e:
                print(f"  迭代 {i + 1} 失败: {e}")
                # 继续下一次迭代
                continue

        return latencies_ms, total_rows, total_bytes

    def run_scenario(
        self,
        scenario_name: str,
        data_type: str = "financial",
        num_batches: int = 10,
        rows_per_batch: int = 1000,
        topic_prefix: str = "benchmark",
    ) -> BenchmarkResult:
        """运行一个测试场景"""
        print(f"\n{'=' * 60}")
        print(f"运行场景: {scenario_name}")
        print(f"{'=' * 60}")

        # 获取配置
        preset = self.config_presets[scenario_name]
        config_name = preset["name"]
        config = preset["config"]

        print(f"配置: {config_name}")
        print(f"描述: {preset['description']}")
        print(f"参数: {json.dumps(config, indent=2)}")

        # 创建 Sink
        sink = self.create_sink(config)

        # 生成数据
        print(f"\n生成数据: {data_type}, {rows_per_batch} 行/批次, {num_batches} 批次")

        if data_type == "financial":
            table = self.data_generator.generate_financial_data(rows_per_batch)
        elif data_type == "log":
            table = self.data_generator.generate_log_data(rows_per_batch)
        elif data_type == "binary":
            table = self.data_generator.generate_binary_data(rows_per_batch)
        else:
            raise ValueError(f"不支持的数据类型: {data_type}")

        table_bytes = self.data_generator.get_table_size_bytes(table)
        print(f"数据大小: {table_bytes:,} 字节 ({table_bytes / (1024 * 1024):.2f} MB)")

        # 运行测试
        print("\n开始性能测试...")
        start_time = time.time()

        topic = (
            f"{topic_prefix}_{scenario_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        )

        latencies_ms, total_rows, total_bytes = self.run_single_test(
            sink=sink, table=table, topic=topic, num_iterations=num_batches
        )

        end_time = time.time()
        duration_seconds = end_time - start_time

        # 收集统计信息
        stats = sink.stats()

        # 计算统计指标
        if latencies_ms:
            latency_p50 = statistics.median(latencies_ms)
            latency_p95 = sorted(latencies_ms)[int(len(latencies_ms) * 0.95)]
            latency_p99 = sorted(latencies_ms)[int(len(latencies_ms) * 0.99)]
        else:
            latency_p50 = latency_p95 = latency_p99 = 0

        rows_per_second = total_rows / duration_seconds if duration_seconds > 0 else 0
        bytes_per_second = total_bytes / duration_seconds if duration_seconds > 0 else 0

        # 估算内存使用（这里使用简单的估算）
        memory_usage_mb = (table_bytes * num_batches) / (1024 * 1024 * 10)  # 简化的估算

        # 创建结果
        result = BenchmarkResult(
            scenario=scenario_name,
            config_name=config_name,
            total_rows=total_rows,
            total_bytes=total_bytes,
            duration_seconds=duration_seconds,
            rows_per_second=rows_per_second,
            bytes_per_second=bytes_per_second,
            latency_p50_ms=latency_p50,
            latency_p95_ms=latency_p95,
            latency_p99_ms=latency_p99,
            memory_usage_mb=memory_usage_mb,
            sr_hit_rate=stats.sr_hit_rate(),
            timestamp=datetime.now().isoformat(),
        )

        # 清理
        sink.close()

        return result

    def run_parameter_sweep(
        self,
        base_scenario: str = "balanced",
        param_name: str = "batch_size",
        param_values: List[int] = None,
        data_type: str = "financial",
        num_batches: int = 5,
        rows_per_batch: int = 1000,
    ) -> List[BenchmarkResult]:
        """运行参数扫描测试"""
        if param_values is None:
            if param_name == "batch_size":
                param_values = [16384, 65536, 262144, 1048576]
            elif param_name == "linger_ms":
                param_values = [0, 5, 20, 100]
            elif param_name == "max_in_flight":
                param_values = [1, 5, 50, 500, 1000]
            else:
                param_values = [1, 10, 100]

        results = []

        print(f"\n{'=' * 60}")
        print(f"参数扫描: {param_name}")
        print(f"{'=' * 60}")

        for param_value in param_values:
            print(f"\n测试 {param_name} = {param_value}")

            # 获取基础配置
            base_config = self.config_presets[base_scenario]["config"].copy()
            base_config[param_name] = param_value

            # 创建临时配置
            temp_config = {
                "name": f"{base_scenario}_{param_name}_{param_value}",
                "description": f"{param_name}={param_value}",
                "config": base_config,
            }

            # 临时修改配置预设
            original_preset = self.config_presets.get("temp", None)
            self.config_presets["temp"] = temp_config

            # 运行测试
            try:
                result = self.run_scenario(
                    scenario_name="temp",
                    data_type=data_type,
                    num_batches=num_batches,
                    rows_per_batch=rows_per_batch,
                    topic_prefix=f"param_sweep_{param_name}",
                )
                results.append(result)
            except Exception as e:
                print(f"  参数 {param_name}={param_value} 测试失败: {e}")

            # 恢复原始配置
            if original_preset:
                self.config_presets["temp"] = original_preset
            else:
                self.config_presets.pop("temp", None)

        return results

    def compare_scenarios(
        self,
        scenario_names: List[str] = None,
        data_type: str = "financial",
        num_batches: int = 10,
        rows_per_batch: int = 1000,
    ) -> List[BenchmarkResult]:
        """比较多个场景"""
        if scenario_names is None:
            scenario_names = ["low_latency", "high_throughput", "reliable", "balanced"]

        results = []

        for scenario_name in scenario_names:
            if scenario_name in self.config_presets:
                try:
                    result = self.run_scenario(
                        scenario_name=scenario_name,
                        data_type=data_type,
                        num_batches=num_batches,
                        rows_per_batch=rows_per_batch,
                    )
                    results.append(result)
                except Exception as e:
                    print(f"场景 {scenario_name} 测试失败: {e}")

        return results

    def print_results(self, results: List[BenchmarkResult], output_file: str = None):
        """打印测试结果"""
        print(f"\n{'=' * 80}")
        print("性能基准测试结果")
        print(f"{'=' * 80}")

        # 表格标题
        print(
            "\n"
            + "| "
            + " | ".join(
                [
                    "场景",
                    "配置",
                    "总行数",
                    "持续时间(s)",
                    "吞吐量(行/秒)",
                    "吞吐量(MB/秒)",
                    "P50延迟(ms)",
                    "P95延迟(ms)",
                    "缓存命中率",
                ]
            )
            + " |"
        )

        print("|-" + "-|-".join(["-" * 10 for _ in range(9)]) + "-|")

        # 数据行
        for result in results:
            print(
                "| "
                + " | ".join(
                    [
                        result.scenario[:10],
                        result.config_name[:10],
                        f"{result.total_rows:,}",
                        f"{result.duration_seconds:.1f}",
                        f"{result.rows_per_second:,.0f}",
                        f"{result.throughput_mbps:.2f}",
                        f"{result.latency_p50_ms:.1f}",
                        f"{result.latency_p95_ms:.1f}",
                        f"{result.sr_hit_rate:.1%}",
                    ]
                )
                + " |"
            )

        # 汇总统计
        if len(results) > 1:
            print(f"\n{'=' * 80}")
            print("汇总统计:")

            best_throughput = max(results, key=lambda x: x.rows_per_second)
            best_latency = min(results, key=lambda x: x.latency_p50_ms)

            print(
                f"  最高吞吐量: {best_throughput.config_name} "
                f"({best_throughput.rows_per_second:,.0f} 行/秒)"
            )
            print(
                f"  最低延迟: {best_latency.config_name} "
                f"({best_latency.latency_p50_ms:.1f} ms P50)"
            )

        # 保存到文件
        if output_file:
            self.save_results(results, output_file)

    def save_results(self, results: List[BenchmarkResult], output_file: str):
        """保存结果到文件"""
        # 转换为字典列表
        results_dict = [asdict(result) for result in results]

        # 添加元数据
        output_data = {
            "metadata": {
                "timestamp": datetime.now().isoformat(),
                "kafka_servers": self.kafka_servers,
                "schema_registry_url": self.schema_registry_url,
                "total_tests": len(results),
            },
            "results": results_dict,
        }

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)

        print(f"\n结果已保存到: {output_file}")

    def print_recommendations(self, results: List[BenchmarkResult]):
        """根据结果提供建议"""
        print(f"\n{'=' * 80}")
        print("配置建议")
        print(f"{'=' * 80}")

        if not results:
            print("没有可用的测试结果")
            return

        # 按场景分组
        scenarios = {}
        for result in results:
            if result.scenario not in scenarios:
                scenarios[result.scenario] = []
            scenarios[result.scenario].append(result)

        for scenario_name, scenario_results in scenarios.items():
            if not scenario_results:
                continue

            # 取第一个结果（假设同一场景配置相同）
            result = scenario_results[0]

            print(f"\n📊 {scenario_name.upper()} ({result.config_name}):")
            print(
                f"   吞吐量: {result.rows_per_second:,.0f} 行/秒 "
                f"({result.throughput_mbps:.2f} MB/秒)"
            )
            print(
                f"   延迟: P50={result.latency_p50_ms:.1f}ms, "
                f"P95={result.latency_p95_ms:.1f}ms"
            )
            print(f"   缓存效率: {result.sr_hit_rate:.1%}")

            # 提供建议
            if scenario_name == "low_latency":
                print("   💡 建议: 适用于实时交易、监控告警等低延迟场景")
                print("           • 需要毫秒级响应时间")
                print("           • 对数据丢失敏感度低")
                print("           • 网络质量要求高")
            elif scenario_name == "high_throughput":
                print("   💡 建议: 适用于数据仓库ETL、日志聚合等高吞吐场景")
                print("           • 批量处理大量数据")
                print("           • 可以接受较高延迟")
                print("           • 网络带宽可能受限")
            elif scenario_name == "reliable":
                print("   💡 建议: 适用于金融交易、订单处理等关键数据场景")
                print("           • 需要数据不丢失、不重复")
                print("           • 可以接受适中的延迟")
                print("           • 网络稳定性重要")
            elif scenario_name == "balanced":
                print("   💡 建议: 适用于大多数通用场景")
                print("           • 平衡延迟和吞吐")
                print("           • 适用于不确定的工作负载")
                print("           • 作为基准配置")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description="Arrow-Kafka-Pyo3 性能基准测试",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--kafka-servers",
        default="localhost:9092",
        help="Kafka 服务器地址 (默认: localhost:9092)",
    )

    parser.add_argument(
        "--schema-registry-url",
        default="http://localhost:8081",
        help="Schema Registry URL (默认: http://localhost:8081)",
    )

    parser.add_argument(
        "--scenario",
        default="all",
        choices=[
            "all",
            "low_latency",
            "high_throughput",
            "reliable",
            "balanced",
            "param_sweep",
        ],
        help="测试场景 (默认: all)",
    )

    parser.add_argument(
        "--data-type",
        default="financial",
        choices=["financial", "log", "binary"],
        help="数据类型 (默认: financial)",
    )

    parser.add_argument(
        "--num-batches", type=int, default=10, help="每个测试的批次数量 (默认: 10)"
    )

    parser.add_argument(
        "--rows-per-batch", type=int, default=1000, help="每个批次的行数 (默认: 1000)"
    )

    parser.add_argument(
        "--duration",
        type=int,
        default=None,
        help="测试持续时间（秒），覆盖 num-batches",
    )

    parser.add_argument("--output", default=None, help="输出结果文件 (JSON 格式)")

    parser.add_argument(
        "--param-sweep",
        default="batch_size",
        choices=["batch_size", "linger_ms", "max_in_flight", "compression_type"],
        help="参数扫描测试的参数",
    )

    parser.add_argument("--compare", action="store_true", help="比较所有预设配置")

    args = parser.parse_args()

    print("Arrow-Kafka-Pyo3 性能基准测试")
    print("=" * 60)
    print(f"Kafka 服务器: {args.kafka_servers}")
    print(f"Schema Registry: {args.schema_registry_url}")
    print(f"数据类型: {args.data_type}")
    print(f"批次: {args.num_batches} × {args.rows_per_batch} 行")

    # 创建基准测试实例
    benchmark = PerformanceBenchmark(
        kafka_servers=args.kafka_servers, schema_registry_url=args.schema_registry_url
    )

    results = []

    try:
        # 根据参数运行测试
        if args.scenario == "all" or args.compare:
            # 比较所有场景
            print("\n运行场景比较测试...")
            scenario_names = ["low_latency", "high_throughput", "reliable", "balanced"]
            results = benchmark.compare_scenarios(
                scenario_names=scenario_names,
                data_type=args.data_type,
                num_batches=args.num_batches,
                rows_per_batch=args.rows_per_batch,
            )

        elif args.scenario == "param_sweep":
            # 参数扫描测试
            print(f"\n运行参数扫描测试: {args.param_sweep}")
            results = benchmark.run_parameter_sweep(
                base_scenario="balanced",
                param_name=args.param_sweep,
                data_type=args.data_type,
                num_batches=args.num_batches,
                rows_per_batch=args.rows_per_batch,
            )

        else:
            # 单个场景测试
            print(f"\n运行单个场景测试: {args.scenario}")
            result = benchmark.run_scenario(
                scenario_name=args.scenario,
                data_type=args.data_type,
                num_batches=args.num_batches,
                rows_per_batch=args.rows_per_batch,
            )
            results = [result]

        # 显示结果
        if results:
            benchmark.print_results(results, args.output)
            benchmark.print_recommendations(results)
        else:
            print("\n⚠️  没有可用的测试结果")
            return 1

    except KeyboardInterrupt:
        print("\n\n测试被用户中断")
        return 130
    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback

        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
