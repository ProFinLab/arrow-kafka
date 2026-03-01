from __future__ import annotations

import pyarrow as pa

# ---------------------------------------------------------------------------
# Exception hierarchy
#
#   ArrowKafkaError (base, extends RuntimeError)
#   ├── SchemaRegistryError
#   ├── SerializationError
#   ├── EnqueueError
#   ├── FlushTimeoutError
#   ├── UnsupportedTypeError
#   └── ConfigError
# ---------------------------------------------------------------------------

class ArrowKafkaError(RuntimeError):
    """Base class for all arrow-kafka-pyo3 errors.

    Catch this to handle any error raised by ``ArrowKafkaSink`` without
    distinguishing between categories.
    """

class SchemaRegistryError(ArrowKafkaError):
    """Schema Registry registration or lookup failed.

    Raised when the sink cannot POST the inferred Avro schema to the
    configured Schema Registry URL, or when the registry returns an error
    response.

    ``str(exc)`` contains the subject name and the underlying HTTP / client
    error message in the form::

        schema registry error [subject={subject}]: {cause}
    """

class SerializationError(ArrowKafkaError):
    """Row-level Avro serialisation failed.

    Raised when a row cannot be converted to an Avro datum — for example
    because of a type mismatch, a ``NULL`` value in a non-nullable field,
    or an Arrow type that is not yet supported by the converter.

    ``str(exc)`` contains the topic, the failing field descriptor, and the
    underlying error message in the form::

        serialization error [topic={topic}, field={field}]: {cause}

    ``field`` is ``"row[N]"`` when the failure is detected at row level, or
    ``"<schema>"`` when the Avro converter itself could not be initialised.
    """

class EnqueueError(ArrowKafkaError):
    """Producer enqueue failed or the ``timeout_ms`` deadline was exceeded.

    Raised when librdkafka reports a fatal produce error, or when the
    internal producer queue remains full past the ``timeout_ms`` deadline
    supplied to :meth:`ArrowKafkaSink.consume_arrow`.

    **Partial-count attribute**:
    ``exc.args[1]`` (an :class:`int`) contains the number of rows that were
    successfully enqueued *before* the failure.  Those rows may still be
    delivered to the broker asynchronously; call :meth:`ArrowKafkaSink.flush`
    to drain them if you need certainty::

        try:
            sink.consume_arrow(table, topic="events", timeout_ms=5000)
        except EnqueueError as exc:
            rows_done: int = exc.args[1]
            print(f"Only {rows_done}/{table.num_rows} rows were enqueued")

    ``str(exc)`` has the form::

        enqueue error [topic={topic}, enqueued_so_far={n}]: {cause}
    """

class FlushTimeoutError(ArrowKafkaError):
    """``flush()`` deadline exceeded before all in-flight messages were acknowledged.

    Raised when the ``timeout_ms`` argument to :meth:`ArrowKafkaSink.flush`
    (or the implicit 30-second timeout in :meth:`ArrowKafkaSink.close`)
    elapses before the producer queue is fully drained.

    After this error the producer may still have messages in its internal
    queue.  You can retry ``flush()`` with a longer timeout, or accept
    potential data loss and call ``close()``.

    ``str(exc)`` has the form::

        flush timeout [timeout_ms={n}]: deadline exceeded before all
        in-flight messages were acknowledged
    """

class UnsupportedTypeError(ArrowKafkaError):
    """An Arrow column has a data type not yet supported by the Avro converter.

    Cast the column to a supported type before calling
    :meth:`ArrowKafkaSink.consume_arrow`.

    Supported Arrow types: ``Utf8``, ``LargeUtf8``, ``Int8``–``Int64``,
    ``UInt8``–``UInt64``, ``Float32``, ``Float64``, ``Boolean``.

    ``str(exc)`` has the form::

        unsupported Arrow type [field={name}, type={arrow_type}]: …
    """

class ConfigError(ArrowKafkaError):
    """Invalid sink configuration supplied at construction time.

    Raised when :class:`ArrowKafkaSink.__init__` receives an invalid argument
    combination (e.g. ``max_in_flight=0``, an unknown ``compression_type``,
    or an unrecognised ``subject_name_strategy``), or when librdkafka itself
    rejects the producer configuration.

    ``str(exc)`` has the form::

        configuration error: {cause}
    """

# ---------------------------------------------------------------------------
# ArrowKafkaSink
# ---------------------------------------------------------------------------

class ArrowKafkaSink:
    """High-performance Kafka sink that produces ``pyarrow.Table`` data as
    Confluent-framed Avro messages.

    **Message format** — each Kafka value payload is::

        [0x00][schema_id: 4 bytes big-endian][avro_datum]

    This is the Confluent wire format, compatible with:

    * Confluent Schema Registry consumers
    * Redpanda Schema Registry
    * Materialize::

          CREATE SOURCE …
          VALUE FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '…'

    **Delivery model** — :meth:`consume_arrow` enqueues rows into
    librdkafka's internal send buffer and returns as soon as all rows are
    *enqueued* (not broker-acknowledged).  Call :meth:`flush` afterwards to
    block until full broker acknowledgement.

    **GIL behaviour** — both :meth:`consume_arrow` and :meth:`flush` release
    the GIL for their Rust / librdkafka work, so other Python threads are
    not blocked.

    **Thread safety** — a single ``ArrowKafkaSink`` instance may be shared
    across threads.

    Example::

        import pyarrow as pa
        from arrow_kafka_pyo3 import ArrowKafkaSink, FlushTimeoutError

        sink = ArrowKafkaSink(
            kafka_servers="localhost:9092",
            schema_registry_url="http://localhost:8081",
        )
        table = pa.table({"symbol": ["AAPL"], "price": [189.3]})
        sink.consume_arrow(table, topic="quotes", key_cols=["symbol"])
        sink.flush()
        sink.close()
    """

    def __init__(
        self,
        kafka_servers: str,
        schema_registry_url: str,
        max_in_flight: int = 1000,
        linger_ms: int = 20,
        batch_size: int = 65536,
        compression_type: str = "none",
        subject_name_strategy: str = "topic_name",
    ) -> None:
        """Create a new ``ArrowKafkaSink``.

        :param kafka_servers: Comma-separated ``host:port`` bootstrap servers.
        :param schema_registry_url: Base URL of the Confluent-compatible Schema
            Registry (e.g. ``"http://localhost:8081"``).  No trailing slash
            required.
        :param max_in_flight: Maximum number of unacknowledged requests per
            broker connection (``max.in.flight.requests.per.connection``).
            Must be ≥ 1.  Default: ``1000``.
        :param linger_ms: Batching linger time in milliseconds (``linger.ms``).
            Higher values improve throughput at the cost of tail latency.
            Default: ``20``.
        :param batch_size: Maximum size in bytes of a single produce-request
            batch (``batch.size``).  Default: ``65536``.
        :param compression_type: Compression codec.  One of ``"none"``,
            ``"gzip"``, ``"snappy"``, ``"lz4"``, ``"zstd"``.
            Default: ``"none"``.
        :param subject_name_strategy: Controls how the Schema Registry subject
            name is derived from the topic and the Avro record name.

            +------------------------+------------------------------+-------------------+
            | Value                  | Subject pattern              | Materialize compat|
            +========================+==============================+===================+
            | ``"topic_name"``       | ``"{topic}-value"``          | ✓ (default)       |
            +------------------------+------------------------------+-------------------+
            | ``"record_name"``      | ``"{avro_record_name}"``     | only if exists    |
            +------------------------+------------------------------+-------------------+
            | ``"topic_record_name"``| ``"{topic}-{avro_record_name}"`` | only if exists|
            +------------------------+------------------------------+-------------------+

            Default: ``"topic_name"``.

        :raises ConfigError: If any argument is invalid or librdkafka rejects
            the producer configuration.
        """
        ...

    def consume_arrow(
        self,
        table: pa.Table,
        topic: str,
        key_cols: list[str] | None = None,
        key_separator: str = "_",
        timeout_ms: int | None = None,
    ) -> int:
        """Send a ``pyarrow.Table`` to a Kafka topic.

        Each row is serialised to an Avro datum, wrapped in the Confluent wire
        format, and enqueued into librdkafka's internal send buffer.  The call
        returns as soon as all rows are **enqueued** — broker acknowledgement
        happens asynchronously.  Call :meth:`flush` afterwards to wait for
        full delivery confirmation.

        The GIL is released for the duration of serialisation and enqueuing.

        :param table: The ``pyarrow.Table`` to produce.  Must contain only
            columns with supported Arrow types (see :exc:`UnsupportedTypeError`).
        :param topic: Destination Kafka topic name.  The Schema Registry
            subject is derived from this name according to the
            ``subject_name_strategy`` set at construction time.
        :param key_cols: Optional list of column names whose string
            representations are concatenated (joined by ``key_separator``) to
            form the Kafka message key.  Rows where *any* key column is
            ``NULL`` produce a keyless message.  Pass ``None`` (default) for
            all messages to be keyless.
        :param key_separator: Separator inserted between key-column values
            when ``key_cols`` has more than one element.  Default: ``"_"``.
        :param timeout_ms: Optional wall-clock deadline in milliseconds for the
            entire enqueue operation.  If the producer queue is full and the
            deadline elapses before a row can be enqueued, raises
            :exc:`EnqueueError` with ``exc.args[1]`` set to the number of rows
            already enqueued.  When ``None`` (default), the call retries
            indefinitely until every row is enqueued.

        :returns: Number of rows successfully enqueued.  On success this always
            equals ``table.num_rows``.  Partial counts are only visible through
            ``EnqueueError.args[1]`` when an error occurs.

        :raises SchemaRegistryError: Schema registration or lookup failed.
        :raises SerializationError: A row could not be serialised to Avro
            (type error, null in non-nullable field, etc.).
        :raises EnqueueError: librdkafka rejected the message (fatal error),
            or ``timeout_ms`` was exceeded while the queue was full.
            ``exc.args[1]`` contains the number of rows enqueued before the
            failure.
        """
        ...

    def flush(self, timeout_ms: int = 30000) -> None:
        """Flush all in-flight messages to the broker.

        Blocks until every message enqueued before this call has received a
        delivery report (success or permanent failure) from the broker, or
        until ``timeout_ms`` elapses.

        **Delivery guarantee** — when this method returns without raising,
        *all* messages produced by preceding :meth:`consume_arrow` calls have
        been acknowledged by the broker (or a non-retryable delivery error was
        reported for them by librdkafka).  It is safe to call ``flush()`` at
        task-shutdown boundaries to assert end-to-end delivery.

        The GIL is released for the duration of the blocking wait.

        :param timeout_ms: Maximum time to wait in milliseconds.
            Default: ``30000`` (30 seconds).

        :raises FlushTimeoutError: Deadline reached before all in-flight
            messages were drained.  Some messages may still be in the producer
            queue.  Retry ``flush()`` with a longer timeout, or call
            ``close()`` to give up.
        """
        ...

    def close(self) -> None:
        """Flush all in-flight messages and release producer resources.

        Calls :meth:`flush` with a 30-second timeout.  Unlike the previous
        no-op implementation, this method now **blocks** until broker delivery
        is confirmed (or the 30-second timeout elapses).

        The GIL is released for the duration of the blocking flush.

        :raises FlushTimeoutError: If the 30-second flush timeout is exceeded.
        """
        ...
