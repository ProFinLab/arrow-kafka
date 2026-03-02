# Arrow-Kafka-Pyo3 部署指南

## 🏗️ 环境要求

### 1.1 最低系统要求

| 组件 | 最低要求 | 推荐配置 | 说明 |
|------|---------|---------|------|
| **操作系统** | Linux x86_64 (glibc 2.17+) | Linux x86_64 (Ubuntu 20.04+/CentOS 8+) | 支持所有主流 Linux 发行版 |
| **Python** | 3.8+ | 3.9+ | 需要 pyarrow 8.0+ |
| **内存** | 4 GB | 16 GB+ | 每个 Sink 实例约 50-200 MB |
| **CPU** | 2 核心 | 8 核心+ | 支持多线程并行处理 |
| **磁盘** | 10 GB | 100 GB+ | 用于日志和临时文件 |

### 1.2 依赖软件

```bash
# 基础依赖
sudo apt-get update
sudo apt-get install -y \
    build-essential \
    pkg-config \
    libssl-dev \
    curl \
    git

# Python 依赖
pip install --upgrade pip
pip install pyarrow>=8.0.0 pandas numpy

# Rust 工具链（从源码构建时需要）
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env
```

### 1.3 Kafka 集群要求

| 组件 | 版本 | 配置要求 |
|------|------|---------|
| **Kafka** | 2.8+ 或 Redpanda v23.2+ | 至少 3 个 broker，启用 SSL/TLS |
| **Schema Registry** | Confluent 7.0+ 或 Redpanda Schema Registry | 高可用模式，配置监控 |
| **ZooKeeper** | 3.6+（如使用） | 至少 3 节点，独立部署 |

## 🚀 部署步骤

### 2.1 安装方式选择

#### 方式一：从 PyPI 安装（生产推荐）

```bash
# 创建虚拟环境
python -m venv /opt/arrow-kafka/venv
source /opt/arrow-kafka/venv/bin/activate

# 安装包
pip install arrow-kafka-pyo3

# 验证安装
python -c "from arrow_kafka_pyo3 import ArrowKafkaSink; print('✅ 安装成功')"
```

#### 方式二：从源码构建（自定义需求）

```bash
# 克隆项目
git clone https://github.com/your-org/arrow-kafka.git /opt/arrow-kafka
cd /opt/arrow-kafka

# 使用 maturin 构建
cd crates/arrow-kafka-pyo3
pip install maturin
maturin build --release

# 安装生成的 wheel 包
pip install target/wheels/arrow_kafka_pyo3-*.whl
```

### 2.2 配置管理

#### 2.2.1 环境变量配置

```bash
# /etc/environment 或 ~/.bashrc
export KAFKA_BOOTSTRAP_SERVERS="kafka1:9092,kafka2:9092,kafka3:9092"
export SCHEMA_REGISTRY_URL="http://schema-registry:8081"
export ARROW_KAFKA_LOG_LEVEL="INFO"
export ARROW_KAFKA_METRICS_PORT="9090"
```

#### 2.2.2 配置文件

```yaml
# /etc/arrow-kafka/config.yaml
kafka:
  bootstrap_servers: "kafka1:9092,kafka2:9092,kafka3:9092"
  security:
    ssl_enabled: true
    ssl_ca_location: "/etc/ssl/certs/ca-certificates.crt"
    ssl_certificate_location: "/etc/ssl/certs/client.pem"
    ssl_key_location: "/etc/ssl/private/client.key"
    
schema_registry:
  url: "http://schema-registry:8081"
  auth:
    username: "${SCHEMA_REGISTRY_USER}"
    password: "${SCHEMA_REGISTRY_PASS}"
    
performance:
  linger_ms: 20
  batch_size: 65536
  compression_type: "lz4"
  max_in_flight: 1000
  
reliability:
  enable_idempotence: true
  acks: "all"
  retries: 10
  request_timeout_ms: 30000
  
monitoring:
  stats_interval_sec: 5
  prometheus_port: 9090
  health_check_endpoint: "/health"
```

### 2.3 服务化部署

#### Systemd 服务配置

```ini
# /etc/systemd/system/arrow-kafka-producer.service
[Unit]
Description=Arrow Kafka Producer Service
After=network.target kafka.service
Requires=kafka.service

[Service]
Type=simple
User=kafka-producer
Group=kafka-producer
WorkingDirectory=/opt/arrow-kafka
EnvironmentFile=/etc/arrow-kafka/environment
ExecStart=/opt/arrow-kafka/venv/bin/python /opt/arrow-kafka/app/main.py
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal

# 资源限制
MemoryMax=2G
CPUQuota=200%
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

#### Docker 部署

```dockerfile
# Dockerfile
FROM python:3.9-slim

# 安装系统依赖
RUN apt-get update && apt-get install -y \
    build-essential \
    pkg-config \
    libssl-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# 安装 Python 包
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 安装 arrow-kafka-pyo3
RUN pip install arrow-kafka-pyo3

# 复制应用代码
COPY app /app
WORKDIR /app

# 健康检查
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD python -c "from arrow_kafka_pyo3 import ArrowKafkaSink; import sys; sys.exit(0)"

# 运行应用
CMD ["python", "main.py"]
```

```yaml
# docker-compose.prod.yml
version: '3.8'
services:
  arrow-kafka-producer:
    build: .
    image: arrow-kafka-producer:v1.0.0
    container_name: arrow-kafka-producer
    restart: unless-stopped
    environment:
      - KAFKA_BOOTSTRAP_SERVERS=kafka1:9092,kafka2:9092,kafka3:9092
      - SCHEMA_REGISTRY_URL=http://schema-registry:8081
      - LOG_LEVEL=INFO
    volumes:
      - ./config:/app/config:ro
      - ./logs:/var/log/arrow-kafka
    ports:
      - "9090:9090"  # 监控端口
    networks:
      - kafka-network
    depends_on:
      - schema-registry
    deploy:
      resources:
        limits:
          memory: 2G
          cpus: '2'
      replicas: 3
      update_config:
        parallelism: 1
        delay: 30s

networks:
  kafka-network:
    external: true
```

### 2.4 Kubernetes 部署

```yaml
# kubernetes/deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: arrow-kafka-producer
  namespace: data-pipeline
spec:
  replicas: 3
  selector:
    matchLabels:
      app: arrow-kafka-producer
  template:
    metadata:
      labels:
        app: arrow-kafka-producer
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "9090"
    spec:
      serviceAccountName: arrow-kafka-sa
      containers:
      - name: producer
        image: registry.example.com/arrow-kafka-producer:v1.0.0
        imagePullPolicy: IfNotPresent
        ports:
        - containerPort: 9090
          name: metrics
        env:
        - name: KAFKA_BOOTSTRAP_SERVERS
          valueFrom:
            configMapKeyRef:
              name: kafka-config
              key: bootstrap.servers
        - name: SCHEMA_REGISTRY_URL
          value: "http://schema-registry.schema-registry.svc.cluster.local:8081"
        - name: LOG_LEVEL
          value: "INFO"
        resources:
          requests:
            memory: "512Mi"
            cpu: "500m"
          limits:
            memory: "2Gi"
            cpu: "2000m"
        volumeMounts:
        - name: config-volume
          mountPath: /app/config
          readOnly: true
        - name: logs-volume
          mountPath: /var/log/arrow-kafka
        livenessProbe:
          httpGet:
            path: /health
            port: 9090
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /ready
            port: 9090
          initialDelaySeconds: 5
          periodSeconds: 5
      volumes:
      - name: config-volume
        configMap:
          name: arrow-kafka-config
      - name: logs-volume
        emptyDir: {}
---
# ConfigMap
apiVersion: v1
kind: ConfigMap
metadata:
  name: arrow-kafka-config
  namespace: data-pipeline
data:
  config.yaml: |
    kafka:
      bootstrap_servers: "kafka1:9092,kafka2:9092,kafka3:9092"
      security:
        ssl_enabled: true
    performance:
      linger_ms: 20
      batch_size: 65536
---
# Service
apiVersion: v1
kind: Service
metadata:
  name: arrow-kafka-producer
  namespace: data-pipeline
spec:
  selector:
    app: arrow-kafka-producer
  ports:
  - port: 9090
    targetPort: 9090
    name: metrics
  - port: 8080
    targetPort: 8080
    name: http
```

## 🔧 配置管理

### 3.1 多环境配置

```python
# config_manager.py
import os
from enum import Enum
from typing import Dict, Any
import yaml

class Environment(Enum):
    DEVELOPMENT = "dev"
    STAGING = "staging"
    PRODUCTION = "prod"

class ConfigManager:
    def __init__(self, env: Environment):
        self.env = env
        self.config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """加载环境特定配置"""
        config_dir = os.path.join(os.path.dirname(__file__), "config")
        
        # 加载基础配置
        with open(os.path.join(config_dir, "base.yaml"), "r") as f:
            base_config = yaml.safe_load(f)
        
        # 加载环境特定配置
        env_file = os.path.join(config_dir, f"{self.env.value}.yaml")
        if os.path.exists(env_file):
            with open(env_file, "r") as f:
                env_config = yaml.safe_load(f)
            base_config.update(env_config)
        
        return base_config
    
    def get_kafka_config(self) -> Dict[str, Any]:
        """获取 Kafka 配置"""
        kafka_config = self.config["kafka"].copy()
        
        # 从环境变量覆盖敏感信息
        if "KAFKA_BOOTSTRAP_SERVERS" in os.environ:
            kafka_config["bootstrap_servers"] = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
        
        return kafka_config
    
    def create_sink_config(self) -> Dict[str, Any]:
        """创建 Sink 配置"""
        return {
            "kafka_servers": self.get_kafka_config()["bootstrap_servers"],
            "schema_registry_url": self.config["schema_registry"]["url"],
            "enable_idempotence": self.config["reliability"]["enable_idempotence"],
            "acks": self.config["reliability"]["acks"],
            "linger_ms": self.config["performance"]["linger_ms"],
            "batch_size": self.config["performance"]["batch_size"],
            "compression_type": self.config["performance"]["compression_type"],
        }
```

### 3.2 配置验证

```python
# config_validator.py
from typing import Dict, Any, List, Tuple
from dataclasses import dataclass
from enum import Enum

class CompressionType(Enum):
    NONE = "none"
    GZIP = "gzip"
    SNAPPY = "snappy"
    LZ4 = "lz4"
    ZSTD = "zstd"

class AcksLevel(Enum):
    ZERO = "0"
    ONE = "1"
    ALL = "all"

@dataclass
class SinkConfig:
    kafka_servers: str
    schema_registry_url: str
    max_in_flight: int = 1000
    linger_ms: int = 20
    batch_size: int = 65536
    compression_type: CompressionType = CompressionType.NONE
    enable_idempotence: bool = False
    acks: AcksLevel = AcksLevel.ONE
    
    def validate(self) -> List[str]:
        """验证配置有效性"""
        errors = []
        
        # 验证 Kafka 服务器
        if not self.kafka_servers or "," not in self.kafka_servers:
            errors.append("Kafka servers 必须包含至少两个 broker，用逗号分隔")
        
        # 验证 Schema Registry URL
        if not self.schema_registry_url.startswith(("http://", "https://")):
            errors.append("Schema Registry URL 必须以 http:// 或 https:// 开头")
        
        # 验证幂等性约束
        if self.enable_idempotence and self.max_in_flight > 5:
            errors.append("启用 enable_idempotence 时，max_in_flight 必须 ≤ 5")
        
        # 验证批次大小
        if self.batch_size < 1024 or self.batch_size > 10485760:  # 10MB
            errors.append("batch_size 必须在 1KB 到 10MB 之间")
        
        # 验证 linger_ms
        if self.linger_ms > 60000:  # 1分钟
            errors.append("linger_ms 不能超过 60000 (1分钟)")
        
        return errors
    
    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> Tuple['SinkConfig', List[str]]:
        """从字典创建配置并验证"""
        try:
            # 转换枚举类型
            if "compression_type" in config_dict:
                config_dict["compression_type"] = CompressionType(config_dict["compression_type"])
            if "acks" in config_dict:
                config_dict["acks"] = AcksLevel(config_dict["acks"])
            
            config = cls(**config_dict)
            errors = config.validate()
            return config, errors
            
        except (ValueError, TypeError) as e:
            return None, [str(e)]
```

## 📊 监控与告警

### 4.1 监控指标

#### 4.1.1 内置指标

```python
# metrics_exporter.py
from prometheus_client import start_http_server, Gauge, Counter, Histogram
import time
from typing import Dict, Any
from arrow_kafka_pyo3 import ArrowKafkaSink, SinkStats

class MetricsExporter:
    def __init__(self, port: int = 9090):
        self.port = port
        
        # 定义指标
        self.rows_enqueued_total = Counter(
            'arrow_kafka_rows_enqueued_total',
            'Total rows enqueued',
            ['topic', 'application']
        )
        
        self.rows_enqueued_rate = Gauge(
            'arrow_kafka_rows_enqueued_per_second',
            'Rows enqueued per second',
            ['topic', 'application']
        )
        
        self.sr_cache_hit_rate = Gauge(
            'arrow_kafka_sr_cache_hit_rate',
            'Schema Registry cache hit rate',
            ['application']
        )
        
        self.enqueue_duration = Histogram(
            'arrow_kafka_enqueue_duration_seconds',
            'Time spent enqueuing rows',
            ['topic', 'application'],
            buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0]
        )
        
        self.flush_duration = Histogram(
            'arrow_kafka_flush_duration_seconds',
            'Time spent flushing',
            ['application'],
            buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0]
        )
        
        self.producer_queue_size = Gauge(
            'arrow_kafka_producer_queue_size',
            'Producer internal queue size',
            ['application']
        )
    
    def start(self):
        """启动 Prometheus HTTP 服务器"""
        start_http_server(self.port)
        print(f"📊 监控服务器启动在端口 {self.port}")
    
    def record_enqueue(self, topic: str, application: str, rows: int, duration: float):
        """记录入队操作"""
        self.rows_enqueued_total.labels(topic=topic, application=application).inc(rows)
        self.enqueue_duration.labels(topic=topic, application=application).observe(duration)
    
    def update_stats(self, stats: SinkStats, application: str):
        """更新统计指标"""
        self.sr_cache_hit_rate.labels(application=application).set(stats.sr_hit_rate())
```

#### 4.1.2 Grafana 仪表板配置

```json
{
  "dashboard": {
    "title": "Arrow-Kafka Producer Monitoring",
    "panels": [
      {
        "title": "Rows Enqueued Rate",
        "targets": [{
          "expr": "rate(arrow_kafka_rows_enqueued_total[5m])",
          "legendFormat": "{{topic}}"
        }],
        "type": "graph"
      },
      {
        "title": "Schema Registry Cache Hit Rate",
        "targets": [{
          "expr": "arrow_kafka_sr_cache_hit_rate",
          "legendFormat": "{{application}}"
        }],
        "type": "gauge",
        "thresholds": {
          "steps": [
            {"color": "red", "value": 0},
            {"color": "yellow", "value": 0.9},
            {"color": "green", "value": 0.95}
          ]
        }
      },
      {
        "title": "Enqueue Duration",
        "targets": [{
          "expr": "histogram_quantile(0.95, rate(arrow_kafka_enqueue_duration_seconds_bucket[5m]))",
          "legendFormat": "P95"
        }],
        "type": "graph",
        "yaxis": {
          "format": "s"
        }
      }
    ]
  }
}
```

### 4.2 告警规则

```yaml
# prometheus/alerts.yml
groups:
  - name: arrow-kafka-alerts
    rules:
      - alert: HighSchemaRegistryMissRate
        expr: arrow_kafka_sr_cache_hit_rate < 0.9
        for: 5m
        labels:
          severity: warning
          team: data-platform
        annotations:
          summary: "Schema Registry 缓存命中率低"
          description: "{{ $labels.application }} 的 Schema Registry 缓存命中率低于 90%"
          
      - alert: EnqueueLatencyHigh
        expr: |
          histogram_quantile(0.95, rate(arrow_kafka_enqueue_duration_seconds_bucket[5m])) > 0.5
        for: 2m
        labels:
          severity: warning
          team: data-platform
        annotations:
          summary: "入队延迟过高"
          description: "{{ $labels.topic }} 的 P95 入队延迟超过 500ms"
          
      - alert: ProducerQueueGrowing
        expr: |
          increase(arrow_kafka_rows_enqueued_total[10m]) > 0
          and increase(arrow_kafka_flush_count[10m]) == 0
        for: 5m
        labels:
          severity: critical
          team: data-platform
        annotations:
          summary: "生产者队列持续增长"
          description: "{{ $labels.application }} 10分钟内未调用 flush()，队列可能堆积"
          
      - alert: NoRowsEnqueued
        expr: |
          rate(arrow_kafka_rows_enqueued_total[10m]) == 0
        for: 10m
        labels:
          severity: critical
          team: data-platform
        annotations:
          summary: "无数据入队"
          description: "{{ $labels.application }} 10分钟内无数据入队"
```

### 4.3 日志配置

```python
# logging_config.py
import logging
import logging.handlers
import json
from datetime import datetime

class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }
        
        # 添加额外字段
        if hasattr(record, 'topic'):
            log_record['topic'] = record.topic
        if hasattr(record, 'rows'):
            log_record['rows'] = record.rows
        if hasattr(record, 'duration_ms'):
            log_record['duration_ms'] = record.duration_ms
        
        # 添加异常信息
        if record.exc_info:
            log_record['exception'] = self.formatException(record.exc_info)
        
        return json.dumps(log_record)

def setup_logging(app_name: str, log_level: str = "INFO"):
    """配置结构化日志"""
    logger = logging.getLogger(app_name)
    logger.setLevel(getattr(logging, log_level.upper()))
    
    # 控制台输出（开发环境）
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(JsonFormatter())
    logger.addHandler(console_handler)
    
    # 文件输出（生产环境）
    file_handler = logging.handlers.RotatingFileHandler(
        filename=f"/var/log/arrow-kafka/{app_name}.log",
        maxBytes=10485760,  # 10MB
        backupCount=10
    )
    file_handler.setFormatter(JsonFormatter())
    logger.addHandler(file_handler)
    
    # 系统日志（可选）
    syslog_handler = logging.handlers.SysLogHandler(address='/dev/log')
    syslog_handler.setFormatter(JsonFormatter())
    logger.addHandler(syslog_handler)
    
    return logger

# 使用示例
logger = setup_logging("arrow-kafka-producer", "INFO")

# 结构化日志记录
logger.info(
    "数据入队完成",
    extra={
        'topic': 'financial_events',
        'rows': 1000,
        'duration_ms': 150
    }
)
```

## 🛡️ 高可用性

### 5.1 多实例部署

```python
# ha_deployment.py
import time
import threading
from typing import List
from concurrent.futures import ThreadPoolExecutor, as_completed
from arrow_kafka_pyo3 import ArrowKafkaSink

class HighAvailabilityProducer:
    def __init__(self, configs: List[dict], replicas: int = 3):
        """
        高可用生产者
        
        Args:
            configs: 不同 Kafka 集群的配置列表
            replicas: 每条消息的副本数（1-3）
        """
        if replicas < 1 or replicas > len(configs):
            raise ValueError(f"副本数必须在 1 到 {len(configs)} 之间")
        
        self.configs = configs
        self.replicas = replicas
        self.sinks = [ArrowKafkaSink(**config) for config in configs]
        self.lock = threading.Lock()
        self.active_sinks = list(range(len(self.sinks)))
    
    def produce_with_replication(self, table, topic: str, **kwargs) -> dict:
        """
        多副本生产
        
        Returns:
            包含每个副本结果的字典
        """
        results = {}
        
        with ThreadPoolExecutor(max_workers=self.replicas) as executor:
            # 选择要使用的 sink
            with self.lock:
                selected_indices = self.active_sinks[:self.replicas]
            
            # 提交并行任务
            future_to_index = {}
            for idx in selected_indices:
                future = executor.submit(
                    self._produce_single,
                    sink=self.sinks[idx],
                    table=table,
                    topic=topic,
                    sink_index=idx,
                    **kwargs
                )
                future_to_index[future] = idx
            
            # 收集结果
            for future in as_completed(future_to_index):
                idx = future_to_index[future]
                try:
                    result = future.result()
                    results[f"replica_{idx}"] = {
                        "status": "success",
                        "rows": result
                    }
                except Exception as e:
                    results[f"replica_{idx}"] = {
                        "status": "error",
                        "error": str(e)
                    }
                    
                    # 标记 sink 为不健康
                    with self.lock:
                        if idx in self.active_sinks:
                            self.active_sinks.remove(idx)
        
        return results
    
    def _produce_single(self, sink: ArrowKafkaSink, table, topic: str, sink_index: int, **kwargs):
        """单个 sink 的生产逻辑"""
        try:
            rows = sink.consume_arrow(table, topic=topic, **kwargs)
            sink.flush(timeout_ms=10000)
            return rows
        except Exception as e:
            # 记录错误并重新抛出
            self._handle_sink_error(sink_index, e)
            raise
    
    def _handle_sink_error(self, sink_index: int, error: Exception):
        """处理 sink 错误"""
        print(f"Sink {sink_index} 出错: {error}")
        
        # 可以在这里实现重试逻辑、告警等
        with self.lock:
            if sink_index in self.active_sinks:
                self.active_sinks.remove(sink_index)
                
        # 尝试恢复（例如，重新创建 sink）
        threading.Thread(
            target=self._recover_sink,
            args=(sink_index,),
            daemon=True
        ).start()
    
    def _recover_sink(self, sink_index: int):
        """尝试恢复出错的 sink"""
        time.sleep(5)  # 等待一段时间
        
        try:
            # 重新创建 sink
            new_sink = ArrowKafkaSink(**self.configs[sink_index])
            self.sinks[sink_index] = new_sink
            
            with self.lock:
                if sink_index not in self.active_sinks:
                    self.active_sinks.append(sink_index)
                    self.active_sinks.sort()
            
            print(f"Sink {sink_index} 恢复成功")
        except Exception as e:
            print(f"Sink {sink_index} 恢复失败: {e}")
```

### 5.2 故障转移策略

```python
# failover_strategy.py
from enum import Enum
from typing import Optional, Callable
import time

class FailoverStrategy(Enum):
    """故障转移策略"""
    ACTIVE_PASSIVE = "active_passive"  # 主备模式
    ACTIVE_ACTIVE = "active_active"    # 双活模式
    ROUND_ROBIN = "round_robin"        # 轮询模式

class FailoverManager:
    def __init__(
        self,
        sinks: list,
        strategy: FailoverStrategy = FailoverStrategy.ACTIVE_PASSIVE,
        health_check_interval: int = 30
    ):
        self.sinks = sinks
        self.strategy = strategy
        self.health_check_interval = health_check_interval
        
        self.primary_index = 0
        self.last_health_check = 0
        self.sink_health = [True] * len(sinks)  # 初始都健康
        
        # 启动健康检查
        self._start_health_check()
    
    def get_available_sink(self) -> Optional[ArrowKafkaSink]:
        """根据策略获取可用的 sink"""
        self._check_health_if_needed()
        
        if self.strategy == FailoverStrategy.ACTIVE_PASSIVE:
            # 主备模式：优先使用主节点
            if self.sink_health[self.primary_index]:
                return self.sinks[self.primary_index]
            
            # 主节点故障，使用第一个健康的备用节点
            for i, (sink, healthy) in enumerate(zip(self.sinks, self.sink_health)):
                if healthy and i != self.primary_index:
                    return sink
        
        elif self.strategy == FailoverStrategy.ROUND_ROBIN:
            # 轮询模式：在所有健康节点间轮询
            healthy_indices = [i for i, healthy in enumerate(self.sink_health) if healthy]
            if not healthy_indices:
                return None
            
            # 简单的轮询逻辑
            current_index = healthy_indices[0]
            self._rotate_round_robin()
            return self.sinks[current_index]
        
        elif self.strategy == FailoverStrategy.ACTIVE_ACTIVE:
            # 双活模式：随机选择一个健康节点
            healthy_indices = [i for i, healthy in enumerate(self.sink_health) if healthy]
            if not healthy_indices:
                return None
            
            import random
            return self.sinks[random.choice(healthy_indices)]
        
        return None
    
    def _check_health_if_needed(self):
        """如果需要，执行健康检查"""
        current_time = time.time()
        if current_time - self.last_health_check > self.health_check_interval:
            self._perform_health_check()
            self.last_health_check = current_time
    
    def _perform_health_check(self):
        """执行健康检查"""
        for i, sink in enumerate(self.sinks):
            try:
                # 简单的健康检查：获取统计信息
                stats = sink.stats()
                self.sink_health[i] = True
            except Exception as e:
                print(f"Sink {i} 健康检查失败: {e}")
                self.sink_health[i] = False
    
    def _rotate_round_robin(self):
        """轮询模式下的轮转逻辑"""
        # 实现轮转逻辑
        pass
    
    def _start_health_check(self):
        """启动定期健康检查"""
        import threading
        
        def health_check_loop():
            while True:
                time.sleep(self.health_check_interval)
                self._perform_health_check()
        
        thread = threading.Thread(target=health_check_loop, daemon=True)
        thread.start()
```

## 🔒 安全配置

### 6.1 SSL/TLS 配置

```python
# ssl_config.py
import os
from pathlib import Path
from typing import Optional

class SSLConfig:
    def __init__(
        self,
        ca_cert_path: Optional[str] = None,
        client_cert_path: Optional[str] = None,
        client_key_path: Optional[str] = None,
        ssl_password: Optional[str] = None
    ):
        self.ca_cert_path = ca_cert_path
        self.client_cert_path = client_cert_path
        self.client_key_path = client_key_path
        self.ssl_password = ssl_password
        
        self._validate_paths()
    
    def _validate_paths(self):
        """验证证书文件路径"""
        if self.ca_cert_path and not Path(self.ca_cert_path).exists():
            raise FileNotFoundError(f"CA 证书文件不存在: {self.ca_cert_path}")
        
        if self.client_cert_path and not Path(self.client_cert_path).exists():
            raise FileNotFoundError(f"客户端证书文件不存在: {self.client_cert_path}")
        
        if self.client_key_path and not Path(self.client_key_path).exists():
            raise FileNotFoundError(f"客户端密钥文件不存在: {self.client_key_path}")
    
    def to_rdkafka_config(self) -> dict:
        """转换为 librdkafka 配置"""
        config = {
            "security.protocol": "ssl",
        }
        
        if self.ca_cert_path:
            config["ssl.ca.location"] = self.ca_cert_path
        
        if self.client_cert_path:
            config["ssl.certificate.location"] = self.client_cert_path
        
        if self.client_key_path:
            config["ssl.key.location"] = self.client_key_path
        
        if self.ssl_password:
            config["ssl.key.password"] = self.ssl_password
        
        return config
    
    @classmethod
    def from_environment(cls) -> 'SSLConfig':
        """从环境变量创建配置"""
        return cls(
            ca_cert_path=os.getenv("KAFKA_SSL_CA_LOCATION"),
            client_cert_path=os.getenv("KAFKA_SSL_CERTIFICATE_LOCATION"),
            client_key_path=os.getenv("KAFKA_SSL_KEY_LOCATION"),
            ssl_password=os.getenv("KAFKA_SSL_KEY_PASSWORD")
        )
```

### 6.2 SASL 认证

```python
# sasl_config.py
from enum import Enum
from typing import Optional

class SASLMechanism(Enum):
    PLAIN = "PLAIN"
    SCRAM_SHA_256 = "SCRAM-SHA-256"
    SCRAM_SHA_512 = "SCRAM-SHA-512"
    OAUTHBEARER = "OAUTHBEARER"

class SASLConfig:
    def __init__(
        self,
        username: str,
        password: str,
        mechanism: SASLMechanism = SASLMechanism.SCRAM_SHA_256,
        ssl_enabled: bool = True
    ):
        self.username = username
        self.password = password
        self.mechanism = mechanism
        self.ssl_enabled = ssl_enabled
    
    def to_rdkafka_config(self) -> dict:
        """转换为 librdkafka 配置"""
        config = {
            "sasl.mechanism": self.mechanism.value,
            "sasl.username": self.username,
            "sasl.password": self.password,
        }
        
        if self.ssl_enabled:
            config["security.protocol"] = "SASL_SSL"
        else:
            config["security.protocol"] = "SASL_PLAINTEXT"
        
        return config
    
    @classmethod
    def from_vault(cls, vault_path: str) -> 'SASLConfig':
        """从 Vault 获取认证信息"""
        # 这里实现从 HashiCorp Vault 或其他密钥管理服务获取凭证
        import hvac
        
        client = hvac.Client(url=os.getenv("VAULT_ADDR"))
        client.token = os.getenv("VAULT_TOKEN")
        
        secret = client.secrets.kv.v2.read_secret_version(
            path=vault_path,
            mount_point="secret"
        )
        
        data = secret["data"]["data"]
        
        return cls(
            username=data["username"],
            password=data["password"],
            mechanism=SASLMechanism(data.get("mechanism", "SCRAM-SHA-256"))
        )
```

## 🚨 灾难恢复

### 7.1 备份与恢复

```python
# disaster_recovery.py
import json
import time
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any
import pickle

class DisasterRecoveryManager:
    def __init__(self, backup_dir: str = "/var/backup/arrow-kafka"):
        self.backup_dir = Path(backup_dir)
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        
        # 配置备份策略
        self.backup_config = {
            "max_backup_files": 100,
            "backup_interval_hours": 24,
            "retention_days": 30
        }
    
    def backup_state(self, sink, metadata: Dict[str, Any]):
        """备份 sink 状态"""
        backup_data = {
            "timestamp": datetime.utcnow().isoformat(),
            "metadata": metadata,
            "stats": {
                "enqueued_total": sink.stats().enqueued_total,
                "flush_count": sink.stats().flush_count,
                "sr_cache_hits": sink.stats().sr_cache_hits,
                "sr_cache_misses": sink.stats().sr_cache_misses,
            }
        }
        
        # 生成备份文件名
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = self.backup_dir / f"backup_{timestamp}.json"
        
        # 保存备份
        with open(backup_file, 'w') as f:
            json.dump(backup_data, f, indent=2)
        
        # 清理旧备份
        self._cleanup_old_backups()
        
        return backup_file
    
    def restore_from_backup(self, sink, backup_file: Path):
        """从备份恢复"""
        if not backup_file.exists():
            raise FileNotFoundError(f"备份文件不存在: {backup_file}")
        
        with open(backup_file, 'r') as f:
            backup_data = json.load(f)
        
        print(f"从备份恢复: {backup_file}")
        print(f"备份时间: {backup_data['timestamp']}")
        print(f"备份统计: {backup_data['stats']}")
        
        # 这里可以实现实际的恢复逻辑
        # 例如：重新发送未确认的消息等
        
        return backup_data
    
    def _cleanup_old_backups(self):
        """清理旧备份文件"""
        backup_files = sorted(self.backup_dir.glob("backup_*.json"))
        
        # 保留最新的 N 个文件
        if len(backup_files) > self.backup_config["max_backup_files"]:
            files_to_delete = backup_files[:-self.backup_config["max_backup_files"]]
            for file in files_to_delete:
                file.unlink()
                print(f"删除旧备份: {file}")
    
    def create_recovery_plan(self) -> Dict[str, Any]:
        """创建灾难恢复计划"""
        return {
            "recovery_objective": {
                "rto": "2 hours",  # 恢复时间目标
                "rpo": "15 minutes"  # 恢复点目标
            },
            "recovery_steps": [
                {
                    "step": 1,
                    "description": "评估损坏程度",
                    "actions": [
                        "检查 Kafka 集群状态",
                        "检查 Schema Registry 状态",
                        "验证备份完整性"
                    ]
                },
                {
                    "step": 2,
                    "description": "恢复基础设施",
                    "actions": [
                        "启动备用 Kafka 集群",
                        "恢复 Schema Registry",
                        "验证网络连接"
                    ]
                },
                {
                    "step": 3,
                    "description": "恢复应用程序",
                    "actions": [
                        "部署应用程序镜像",
                        "应用最新配置",
                        "从备份恢复状态"
                    ]
                },
                {
                    "step": 4,
                    "description": "验证恢复",
                    "actions": [
                        "运行健康检查",
                        "发送测试消息",
                        "验证端到端流程"
                    ]
                }
            ]
        }
```

### 7.2 监控恢复过程

```python
# recovery_monitor.py
import time
from typing import Callable, List
from dataclasses import dataclass
from enum import Enum

class RecoveryStatus(Enum):
    NOT_STARTED = "not_started"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"

@dataclass
class RecoveryStep:
    name: str
    description: str
    status: RecoveryStatus = RecoveryStatus.NOT_STARTED
    start_time: float = None
    end_time: float = None
    error: str = None

class RecoveryMonitor:
    def __init__(self):
        self.steps: List[RecoveryStep] = []
        self.current_step_index = -1
        self.overall_status = RecoveryStatus.NOT_STARTED
    
    def add_step(self, name: str, description: str):
        """添加恢复步骤"""
        step = RecoveryStep(name=name, description=description)
        self.steps.append(step)
    
    def start_step(self, step_index: int):
        """开始执行步骤"""
        if step_index < 0 or step_index >= len(self.steps):
            raise IndexError(f"无效的步骤索引: {step_index}")
        
        step = self.steps[step_index]
        step.status = RecoveryStatus.IN_PROGRESS
        step.start_time = time.time()
        self.current_step_index = step_index
        
        print(f"▶️ 开始恢复步骤: {step.name}")
        print(f"   {step.description}")
    
    def complete_step(self, step_index: int):
        """完成步骤"""
        step = self.steps[step_index]
        step.status = RecoveryStatus.COMPLETED
        step.end_time = time.time()
        
        duration = step.end_time - step.start_time
        print(f"✅ 完成恢复步骤: {step.name} ({duration:.1f}s)")
    
    def fail_step(self, step_index: int, error: str):
        """标记步骤失败"""
        step = self.steps[step_index]
        step.status = RecoveryStatus.FAILED
        step.end_time = time.time()
        step.error = error
        
        print(f"❌ 恢复步骤失败: {step.name}")
        print(f"   错误: {error}")
    
    def generate_report(self) -> dict:
        """生成恢复报告"""
        completed_steps = [s for s in self.steps if s.status == RecoveryStatus.COMPLETED]
        failed_steps = [s for s in self.steps if s.status == RecoveryStatus.FAILED]
        
        total_time = 0
        if completed_steps:
            total_time = max(s.end_time for s in completed_steps) - min(s.start_time for s in self.steps)
        
        return {
            "overall_status": self.overall_status.value,
            "total_steps": len(self.steps),
            "completed_steps": len(completed_steps),
            "failed_steps": len(failed_steps),
            "total_time_seconds": total_time,
            "steps": [
                {
                    "name": s.name,
                    "status": s.status.value,
                    "duration": s.end_time - s.start_time if s.end_time else None,
                    "error": s.error
                }
                for s in self.steps
            ]
        }
```

## 🎯 性能优化

### 8.1 批量处理优化

```python
# batch_optimizer.py
from typing import List, Optional
import time
from dataclasses import dataclass
from statistics import mean, median

@dataclass
class BatchMetrics:
    batch_size: int
    processing_time: float
    rows_per_second: float
    memory_usage_mb: float

class BatchOptimizer:
    def __init__(self, initial_batch_size: int = 1000):
        self.batch_size = initial_batch_size
        self.metrics_history: List[BatchMetrics] = []
        self.optimization_interval = 10  # 每10个批次优化一次
        
    def record_batch(self, batch_size: int, processing_time: float, memory_usage_mb: float):
        """记录批次指标"""
        metrics = BatchMetrics(
            batch_size=batch_size,
            processing_time=processing_time,
            rows_per_second=batch_size / processing_time if processing_time > 0 else 0,
            memory_usage_mb=memory_usage_mb
        )
        
        self.metrics_history.append(metrics)
        
        # 定期优化批次大小
        if len(self.metrics_history) % self.optimization_interval == 0:
            self._optimize_batch_size()
    
    def _optimize_batch_size(self):
        """优化批次大小"""
        if len(self.metrics_history) < self.optimization_interval:
            return
        
        recent_metrics = self.metrics_history[-self.optimization_interval:]
        
        # 计算关键指标
        avg_throughput = mean(m.rows_per_second for m in recent_metrics)
        avg_memory = mean(m.memory_usage_mb for m in recent_metrics)
        avg_latency = mean(m.processing_time for m in recent_metrics)
        
        # 优化逻辑
        if avg_memory > 500:  # 内存使用超过500MB
            # 减少批次大小
            self.batch_size = max(100, int(self.batch_size * 0.8))
            print(f"内存使用过高，减少批次大小到 {self.batch_size}")
        
        elif avg_latency < 0.1 and avg_memory < 300:  # 延迟低且内存充足
            # 增加批次大小
            self.batch_size = min(10000, int(self.batch_size * 1.2))
            print(f"性能良好，增加批次大小到 {self.batch_size}")
        
        print(f"优化结果: 吞吐={avg_throughput:.0f}行/秒, "
              f"内存={avg_memory:.1f}MB, 延迟={avg_latency:.3f}s")
    
    def get_recommendations(self) -> List[str]:
        """获取优化建议"""
        recommendations = []
        
        if len(self.metrics_history) < 5:
            return ["需要更多数据进行分析"]
        
        recent = self.metrics_history[-5:]
        
        # 检查内存使用
        avg_memory = mean(m.memory_usage_mb for m in recent)
        if avg_memory > 1000:
            recommendations.append("内存使用过高，考虑减少批次大小或增加 flush 频率")
        
        # 检查吞吐量
        avg_throughput = mean(m.rows_per_second for m in recent)
        if avg_throughput < 1000:
            recommendations.append("吞吐量较低，考虑增加批次大小或启用压缩")
        
        # 检查延迟
        avg_latency = mean(m.processing_time for m in recent)
        if avg_latency > 1.0:
            recommendations.append("处理延迟较高，考虑减少批次大小")
        
        return recommendations
```

### 8.2 连接池优化

```python
# connection_pool_optimizer.py
import threading
from typing import Dict, List
from queue import Queue, Empty
import time

class OptimizedConnectionPool:
    def __init__(self, max_pool_size: int = 10, idle_timeout: int = 300):
        self.max_pool_size = max_pool_size
        self.idle_timeout = idle_timeout  # 秒
        
        self.available_sinks: Queue = Queue()
        self.in_use_sinks: Dict[int, 'ArrowKafkaSink'] = {}
        self.sink_timestamps: Dict[int, float] = {}
        
        self.lock = threading.Lock()
        self.cleanup_thread = threading.Thread(target=self._cleanup_idle_sinks, daemon=True)
        self.cleanup_thread.start()
    
    def get_sink(self, config: dict) -> 'ArrowKafkaSink':
        """获取或创建 sink"""
        with self.lock:
            try:
                # 尝试从池中获取
                sink = self.available_sinks.get_nowait()
                sink_id = id(sink)
                
                print(f"复用现有连接: {sink_id}")
            except Empty:
                # 创建新连接
                if len(self.in_use_sinks) + self.available_sinks.qsize() >= self.max_pool_size:
                    raise RuntimeError("连接池已满")
                
                sink = ArrowKafkaSink(**config)
                sink_id = id(sink)
                print(f"创建新连接: {sink_id}")
            
            # 记录使用时间
            self.in_use_sinks[sink_id] = sink
            self.sink_timestamps[sink_id] = time.time()
            
            return sink
    
    def return_sink(self, sink: 'ArrowKafkaSink'):
        """归还 sink 到池中"""
        sink_id = id(sink)
        
        with self.lock:
            if sink_id in self.in_use_sinks:
                del self.in_use_sinks[sink_id]
                
                # 检查 sink 是否健康
                try:
                    stats = sink.stats()
                    self.available_sinks.put(sink)
                    print(f"连接归还到池中: {sink_id}")
                except Exception as e:
                    print(f"连接 {sink_id} 不健康，丢弃: {e}")
                    # 不健康的连接不返回池中
    
    def _cleanup_idle_sinks(self):
        """清理空闲超时的连接"""
        while True:
            time.sleep(60)  # 每分钟检查一次
            
            with self.lock:
                current_time = time.time()
                
                # 收集所有空闲连接
                idle_sinks = []
                while not self.available_sinks.empty():
                    try:
                        sink = self.available_sinks.get_nowait()
                        sink_id = id(sink)
                        
                        if current_time - self.sink_timestamps.get(sink_id, 0) > self.idle_timeout:
                            print(f"关闭空闲连接: {sink_id}")
                            sink.close()
                        else:
                            idle_sinks.append(sink)
                    except Empty:
                        break
                
                # 放回未超时的连接
                for sink in idle_sinks:
                    self.available_sinks.put(sink)
```

## 📋 部署检查清单

### 9.1 预部署检查

- [ ] **基础设施**
  - [ ] Kafka 集群健康状态确认
  - [ ] Schema Registry 服务可用
  - [ ] 网络连通性测试通过
  - [ ] 防火墙规则配置正确
  - [ ] SSL/TLS 证书有效

- [ ] **应用程序**
  - [ ] 代码通过所有测试
  - [ ] Docker 镜像构建成功
  - [ ] 配置文件中无敏感信息泄露
  - [ ] 日志配置正确
  - [ ] 监控端点可访问

- [ ] **安全**
  - [ ] 使用最小权限原则
  - [ ] 密钥和证书安全存储
  - [ ] 访问控制列表配置
  - [ ] 审计日志开启

### 9.2 部署后验证

- [ ] **功能验证**
  - [ ] 应用程序启动成功
  - [ ] 能连接到 Kafka 集群
  - [ ] Schema Registry 集成正常
  - [ ] 数据生产和消费测试通过

- [ ] **性能验证**
  - [ ] 吞吐量达到预期
  - [ ] 延迟在可接受范围
  - [ ] 内存使用稳定
  - [ ] CPU 使用率正常

- [ ] **监控验证**
  - [ ] Prometheus 指标可收集
  - [ ] Grafana 仪表板显示正常
  - [ ] 告警规则配置正确
  - [ ] 日志收集工作正常

### 9.3 回滚计划

如果部署出现问题，按以下步骤回滚：

1. **立即停止新版本部署**
2. **恢复上一个稳定版本的配置**
3. **重新启动旧版本服务**
4. **验证数据一致性**
5. **分析部署失败原因**

## 📞 支持与故障排除

### 10.1 常见问题

| 问题 | 可能原因 | 解决方案 |
|------|---------|---------|
| 连接 Kafka 失败 | 网络问题、证书错误、权限不足 | 检查网络连通性、验证证书、检查 ACL |
| Schema Registry 错误 | Schema 不兼容、服务不可用 | 检查 Schema 兼容性、重启服务 |
| 内存使用过高 | 批次过大、未及时 flush | 减小 batch_size、增加 flush 频率 |
| 吞吐量低 | 配置不当、网络延迟 | 优化 linger_ms、启用压缩 |

### 10.2 获取支持

- **紧急问题**: 联系平台运维团队
- **功能问题**: 提交 GitHub Issue
- **文档问题**: 提交文档更新请求
- **安全漏洞**: 通过安全邮箱报告

---

**最后更新**: 2024年1月  
**版本**: 1.0.0  
**作者**: Arrow-Kafka-Pyo3 团队  

> 提示：本指南提供生产环境部署的最佳实践，实际部署时请根据具体环境进行调整。