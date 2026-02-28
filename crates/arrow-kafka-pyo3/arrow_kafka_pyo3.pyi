import pyarrow as pa

class ArrowKafkaSink:
    def __init__(
        self,
        kafka_servers: str,
        schema_registry_url: str,
        max_in_flight: int = 1000,
        linger_ms: int = 20,
        batch_size: int = 65536,
        compression_type: str = "none",
    ) -> None: ...
    def consume_arrow(
        self,
        table: pa.Table,
        topic: str,
        key_cols: list[str] | None = None,
        key_separator: str = "_",
        timeout_ms: int | None = None,
    ) -> int: ...
    def flush(self, timeout_ms: int = 30000) -> int: ...
    def close(self) -> None: ...
