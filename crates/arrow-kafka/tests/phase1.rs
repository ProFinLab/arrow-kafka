//! Phase 1 unit tests — all run fully offline (no broker, no Schema Registry).
//!
//! Coverage:
//!   1. `SinkError` — Display messages contain machine-parseable `[key=value]` tags.
//!   2. `SubjectNameStrategy` — `from_str`, `subject()` for all three variants.
//!   3. `SrClient::subject()` delegation and `confluent_frame` wire format.
//!   4. `ArrowToAvroConverter::schema_name()` — non-empty, stable across two calls.
//!   5. `ArrowKafkaSink::new` — `max_in_flight=0` rejected as `SinkError::Config`.
//!   6. `ArrowKafkaSink::new` — all three `SubjectNameStrategy` variants accepted.
//!   7. `consume_arrow` — empty batch slice returns `Ok(0)` with no I/O.
//!   8. `flush()` — idle producer (nothing enqueued) returns `Ok(())` immediately.
//!   9. `close()` — idle producer returns `Ok(())` (delegates to `flush`).

use std::sync::Arc;

use arrow::array::{BooleanArray, Float32Array, Float64Array, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use arrow_kafka::ArrowToAvroConverter;
use arrow_kafka::error::SinkError;
use arrow_kafka::schema_registry::{SrClient, SubjectNameStrategy};
use arrow_kafka::sink::ArrowKafkaSink;

// ---------------------------------------------------------------------------
// Shared test fixtures
// ---------------------------------------------------------------------------

/// A two-column non-nullable Arrow schema used across multiple tests.
fn stock_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
    ]))
}

/// A two-row `RecordBatch` built from [`stock_schema`].
fn stock_batch() -> RecordBatch {
    let schema = stock_schema();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["AAPL", "TSLA"])),
            Arc::new(Float64Array::from(vec![189.3_f64, 250.0_f64])),
        ],
    )
    .expect("valid batch")
}

/// Construct a sink that will never successfully connect (invalid address).
///
/// librdkafka creates the producer handle eagerly but defers broker connection
/// to the first produce call, so construction itself succeeds even with a
/// bogus address.  This lets us test offline behaviour (idle flush, close,
/// empty-batch fast-path) without real infrastructure.
fn make_offline_sink(
    max_in_flight: usize,
    strategy: SubjectNameStrategy,
) -> Result<ArrowKafkaSink, SinkError> {
    ArrowKafkaSink::new(
        "127.0.0.1:19991".to_string(),        // no broker here
        "http://127.0.0.1:19991".to_string(), // no SR here
        max_in_flight,
        0,     // linger_ms — no batching delay in tests
        65536, // batch_size
        "none".to_string(),
        strategy,
        false,           // enable_idempotence
        "1".to_string(), // acks
        None,            // retries — use librdkafka default
        100,             // retry_backoff_ms
        30_000,          // request_timeout_ms
    )
}

// ===========================================================================
// 1. SinkError — Display format
//
// Rules enforced:
//   - Category label is present (enables `grep "schema registry error"` in logs).
//   - Every structured `[key=value]` tag mentioned in the doc-comment is present.
//   - The free-form cause string is embedded verbatim.
// ===========================================================================

#[test]
fn error_schema_registry_display_contains_all_tags() {
    let e = SinkError::SchemaRegistry {
        subject: "trades-value".to_string(),
        cause: "HTTP 422 Unprocessable Entity".to_string(),
    };
    let s = e.to_string();
    assert!(s.contains("schema registry error"), "label missing: {s}");
    assert!(
        s.contains("[subject=trades-value]"),
        "subject tag missing: {s}"
    );
    assert!(s.contains("HTTP 422"), "cause missing: {s}");
}

#[test]
fn error_serialization_display_contains_all_tags() {
    let e = SinkError::Serialization {
        topic: "quotes".to_string(),
        field: "row[3]".to_string(),
        cause: "null value in non-nullable field 'price'".to_string(),
    };
    let s = e.to_string();
    assert!(s.contains("serialization error"), "label missing: {s}");
    assert!(s.contains("[topic=quotes"), "topic tag missing: {s}");
    assert!(s.contains("field=row[3]"), "field tag missing: {s}");
    assert!(s.contains("non-nullable"), "cause missing: {s}");
}

#[test]
fn error_serialization_schema_level_field_tag() {
    // When the converter itself fails (schema-level, not row-level), field
    // is reported as "<schema>".
    let e = SinkError::Serialization {
        topic: "t".to_string(),
        field: "<schema>".to_string(),
        cause: "unsupported root type".to_string(),
    };
    let s = e.to_string();
    assert!(
        s.contains("field=<schema>"),
        "schema-level field tag missing: {s}"
    );
}

#[test]
fn error_enqueue_display_contains_count_and_topic() {
    let e = SinkError::Enqueue {
        topic: "events".to_string(),
        enqueued_so_far: 42,
        cause: "librdkafka fatal error: broker transport failure".to_string(),
    };
    let s = e.to_string();
    assert!(s.contains("enqueue error"), "label missing: {s}");
    assert!(s.contains("[topic=events"), "topic tag missing: {s}");
    assert!(s.contains("enqueued_so_far=42"), "count tag missing: {s}");
    assert!(s.contains("fatal error"), "cause missing: {s}");
}

#[test]
fn error_flush_timeout_display_contains_ms() {
    let e = SinkError::FlushTimeout { timeout_ms: 5000 };
    let s = e.to_string();
    assert!(s.contains("flush timeout"), "label missing: {s}");
    assert!(s.contains("[timeout_ms=5000]"), "ms tag missing: {s}");
    assert!(s.contains("acknowledged"), "guarantee text missing: {s}");
}

#[test]
fn error_unsupported_type_display_contains_field_and_type() {
    let e = SinkError::UnsupportedType {
        field: "ts".to_string(),
        arrow_type: "Timestamp(Microsecond, None)".to_string(),
    };
    let s = e.to_string();
    assert!(s.contains("unsupported Arrow type"), "label missing: {s}");
    assert!(s.contains("[field=ts"), "field tag missing: {s}");
    assert!(s.contains("type=Timestamp"), "type tag missing: {s}");
}

#[test]
fn error_config_display_contains_cause() {
    let e = SinkError::Config {
        cause: "max_in_flight must be >= 1".to_string(),
    };
    let s = e.to_string();
    assert!(s.contains("configuration error"), "label missing: {s}");
    assert!(
        s.contains("max_in_flight must be >= 1"),
        "cause missing: {s}"
    );
}

// ===========================================================================
// 2. SubjectNameStrategy — from_str and subject()
// ===========================================================================

#[test]
fn strategy_from_str_topic_name() {
    let s = SubjectNameStrategy::from_str("topic_name").expect("valid");
    assert!(matches!(s, SubjectNameStrategy::TopicName));
}

#[test]
fn strategy_from_str_record_name() {
    let s = SubjectNameStrategy::from_str("record_name").expect("valid");
    assert!(matches!(s, SubjectNameStrategy::RecordName));
}

#[test]
fn strategy_from_str_topic_record_name() {
    let s = SubjectNameStrategy::from_str("topic_record_name").expect("valid");
    assert!(matches!(s, SubjectNameStrategy::TopicRecordName));
}

#[test]
fn strategy_from_str_unknown_returns_err_with_hint() {
    let err = SubjectNameStrategy::from_str("bad_value").unwrap_err();
    // Error message must name the invalid input AND list valid alternatives.
    assert!(err.contains("bad_value"), "input not echoed: {err}");
    assert!(err.contains("topic_name"), "valid values not listed: {err}");
    assert!(
        err.contains("record_name"),
        "valid values not listed: {err}"
    );
    assert!(
        err.contains("topic_record_name"),
        "valid values not listed: {err}"
    );
}

#[test]
fn strategy_subject_topic_name() {
    let s = SubjectNameStrategy::TopicName;
    assert_eq!(s.subject("my_topic", "TradeRecord"), "my_topic-value");
}

#[test]
fn strategy_subject_record_name_ignores_topic() {
    let s = SubjectNameStrategy::RecordName;
    assert_eq!(s.subject("my_topic", "TradeRecord"), "TradeRecord");
    // Confirm it's truly topic-independent.
    assert_eq!(s.subject("other_topic", "TradeRecord"), "TradeRecord");
}

#[test]
fn strategy_subject_topic_record_name_combines_both() {
    let s = SubjectNameStrategy::TopicRecordName;
    assert_eq!(s.subject("my_topic", "TradeRecord"), "my_topic-TradeRecord");
}

#[test]
fn strategy_default_is_topic_name() {
    let s = SubjectNameStrategy::default();
    assert!(matches!(s, SubjectNameStrategy::TopicName));
    // Default produces Confluent-standard subject.
    assert_eq!(s.subject("t", "R"), "t-value");
}

// ===========================================================================
// 3. SrClient — subject delegation and confluent_frame wire format
// ===========================================================================

#[test]
fn sr_client_subject_delegates_to_strategy_topic_name() {
    let client = SrClient::new(
        "http://localhost:8081".to_string(),
        SubjectNameStrategy::TopicName,
    )
    .expect("client creation should not fail");
    assert_eq!(client.subject("orders", "OrderRecord"), "orders-value");
}

#[test]
fn sr_client_subject_delegates_to_strategy_record_name() {
    let client = SrClient::new(
        "http://localhost:8081".to_string(),
        SubjectNameStrategy::RecordName,
    )
    .expect("client creation should not fail");
    assert_eq!(client.subject("orders", "OrderRecord"), "OrderRecord");
}

#[test]
fn sr_client_subject_delegates_to_strategy_topic_record_name() {
    let client = SrClient::new(
        "http://localhost:8081".to_string(),
        SubjectNameStrategy::TopicRecordName,
    )
    .expect("client creation should not fail");
    assert_eq!(
        client.subject("orders", "OrderRecord"),
        "orders-OrderRecord"
    );
}

#[test]
fn confluent_frame_wire_format_magic_byte_and_schema_id() {
    // Schema ID 7 → big-endian bytes [0, 0, 0, 7].
    let payload = b"avrodatum";
    let framed = SrClient::confluent_frame(7, payload);

    // Magic byte must be 0x00.
    assert_eq!(framed[0], 0x00, "magic byte must be 0x00");

    // Next 4 bytes are the schema ID in big-endian order.
    let id_bytes: [u8; 4] = framed[1..5].try_into().unwrap();
    assert_eq!(
        u32::from_be_bytes(id_bytes),
        7,
        "schema_id big-endian mismatch"
    );

    // The remainder is the raw Avro datum, byte-for-byte.
    assert_eq!(&framed[5..], payload, "avro datum was corrupted");
}

#[test]
fn confluent_frame_total_length_is_5_plus_payload() {
    let payload = vec![0xAB_u8; 100];
    let framed = SrClient::confluent_frame(1, &payload);
    assert_eq!(framed.len(), 1 + 4 + 100);
}

#[test]
fn confluent_frame_large_schema_id_round_trips() {
    // Schema IDs close to u32::MAX must not overflow.
    let id = 0x00FF_FFFF_u32;
    let framed = SrClient::confluent_frame(id, b"x");
    let id_bytes: [u8; 4] = framed[1..5].try_into().unwrap();
    assert_eq!(u32::from_be_bytes(id_bytes), id);
}

// ===========================================================================
// 4. ArrowToAvroConverter::schema_name()
// ===========================================================================

#[test]
fn converter_schema_name_is_non_empty() {
    let schema = stock_schema();
    let converter = ArrowToAvroConverter::new(schema).expect("valid schema");
    assert!(
        !converter.schema_name().is_empty(),
        "schema_name must never be empty"
    );
}

#[test]
fn converter_schema_name_is_stable_across_calls() {
    let schema = stock_schema();
    let converter = ArrowToAvroConverter::new(schema).expect("valid schema");
    // Calling schema_name() twice must return identical results (no
    // non-deterministic generation).
    assert_eq!(converter.schema_name(), converter.schema_name());
}

#[test]
fn converter_schema_name_is_consistent_for_identical_schemas() {
    // Two independently created converters from the same Arrow schema must
    // produce the same schema_name.
    let c1 = ArrowToAvroConverter::new(stock_schema()).expect("valid");
    let c2 = ArrowToAvroConverter::new(stock_schema()).expect("valid");
    assert_eq!(c1.schema_name(), c2.schema_name());
}

#[test]
fn converter_schema_name_contains_only_identifier_safe_chars() {
    // The schema name will be embedded in subject strings; it must not
    // contain characters that would break subject naming (e.g. raw spaces,
    // slashes, or quotes).
    let schema = stock_schema();
    let converter = ArrowToAvroConverter::new(schema).expect("valid schema");
    let name = converter.schema_name();
    for ch in name.chars() {
        assert!(
            ch.is_alphanumeric() || ch == '_' || ch == '-' || ch == '.',
            "schema_name '{name}' contains unsafe char '{ch}'"
        );
    }
}

// ===========================================================================
// 5 & 6. ArrowKafkaSink::new — configuration validation
// ===========================================================================

#[test]
fn sink_new_max_in_flight_zero_returns_config_error() {
    let err = match make_offline_sink(0, SubjectNameStrategy::TopicName) {
        Ok(_) => panic!("max_in_flight=0 must be accepted — expected rejection"),
        Err(e) => e,
    };
    assert!(
        matches!(err, SinkError::Config { .. }),
        "expected Config error, got: {err}"
    );
    assert!(
        err.to_string().contains("max_in_flight"),
        "error message should mention max_in_flight: {err}"
    );
}

#[test]
fn sink_new_max_in_flight_one_is_valid() {
    make_offline_sink(1, SubjectNameStrategy::TopicName).expect("max_in_flight=1 must be accepted");
}

#[test]
fn sink_new_large_max_in_flight_is_valid() {
    make_offline_sink(1000, SubjectNameStrategy::TopicName)
        .expect("max_in_flight=1000 must be accepted");
}

#[test]
fn sink_new_accepts_topic_name_strategy() {
    make_offline_sink(100, SubjectNameStrategy::TopicName)
        .expect("TopicName strategy must be accepted");
}

#[test]
fn sink_new_accepts_record_name_strategy() {
    make_offline_sink(100, SubjectNameStrategy::RecordName)
        .expect("RecordName strategy must be accepted");
}

#[test]
fn sink_new_accepts_topic_record_name_strategy() {
    make_offline_sink(100, SubjectNameStrategy::TopicRecordName)
        .expect("TopicRecordName strategy must be accepted");
}

// ===========================================================================
// 7. consume_arrow — empty batch slice fast-path (no I/O)
// ===========================================================================

#[test]
fn consume_arrow_empty_slice_returns_zero_without_io() {
    let sink = make_offline_sink(100, SubjectNameStrategy::TopicName)
        .expect("sink construction must succeed");

    // Pass an empty slice — this must short-circuit before any network call
    // (schema registry or broker), so it succeeds even with an invalid
    // bootstrap address.
    let result = sink.consume_arrow(&[], "any_topic", None, "_", None);
    assert_eq!(result.expect("empty slice must return Ok(0)"), 0);
}

// ===========================================================================
// 8 & 9. flush() and close() on an idle producer
//
// When the producer queue is empty, flush() must complete immediately and
// return Ok(()).  close() delegates to flush(30_000), so it must too.
// ===========================================================================

#[test]
fn flush_on_idle_producer_succeeds() {
    let sink = make_offline_sink(100, SubjectNameStrategy::TopicName)
        .expect("sink construction must succeed");

    // Nothing has been enqueued — flush must drain instantly.
    sink.flush(5_000)
        .expect("flush on idle producer must succeed");
}

#[test]
fn close_on_idle_producer_succeeds() {
    let sink = make_offline_sink(100, SubjectNameStrategy::TopicName)
        .expect("sink construction must succeed");

    sink.close().expect("close on idle producer must succeed");
}

#[test]
fn flush_is_idempotent_when_idle() {
    let sink = make_offline_sink(100, SubjectNameStrategy::TopicName)
        .expect("sink construction must succeed");

    // Multiple flush calls on an idle producer must all succeed.
    sink.flush(1_000).expect("first flush must succeed");
    sink.flush(1_000).expect("second flush must succeed");
    sink.flush(1_000).expect("third flush must succeed");
}

// ===========================================================================
// Additional: converter correctly serialises all supported primitive types
// (regression guard — ensures Phase 2 type additions don't break existing ones)
// ===========================================================================

#[test]
fn converter_handles_nullable_fields() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("score", DataType::Float64, true),
    ]));

    let name = StringArray::from(vec![Some("Alice"), None]);
    let score = Float64Array::from(vec![Some(9.9_f64), None]);

    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(name), Arc::new(score)]).unwrap();

    let converter = ArrowToAvroConverter::new(schema).unwrap();

    // Row 0: both fields present.
    let row0 = converter
        .convert_row(&batch, 0)
        .expect("row 0 must serialise");
    assert!(!row0.is_empty());

    // Row 1: both fields NULL — must still serialise (produces Avro union Null).
    let row1 = converter
        .convert_row(&batch, 1)
        .expect("row 1 (all-null) must serialise");
    assert!(!row1.is_empty());
}

#[test]
fn converter_handles_boolean_and_int32_fields() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("flag", DataType::Boolean, false),
        Field::new("count", DataType::Int32, false),
    ]));

    let flags = BooleanArray::from(vec![true, false, true]);
    let counts = Int32Array::from(vec![1_i32, 2, 3]);

    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(flags), Arc::new(counts)]).unwrap();

    let converter = ArrowToAvroConverter::new(schema).unwrap();

    for row in 0..3 {
        let bytes = converter
            .convert_row(&batch, row)
            .unwrap_or_else(|e| panic!("row {row} serialisation failed: {e}"));
        assert!(!bytes.is_empty(), "row {row} produced empty payload");
    }
}

#[test]
fn converter_handles_float32_field() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "f32",
        DataType::Float32,
        false,
    )]));
    let arr = Float32Array::from(vec![1.5_f32, 2.5_f32]);
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(arr)]).unwrap();

    let converter = ArrowToAvroConverter::new(schema).unwrap();
    let bytes = converter
        .convert_row(&batch, 0)
        .expect("Float32 must serialise");
    assert!(!bytes.is_empty());
}

#[test]
fn converter_out_of_bounds_row_returns_error() {
    let schema = stock_schema();
    let batch = stock_batch();
    let converter = ArrowToAvroConverter::new(schema).unwrap();

    // batch has 2 rows; index 2 is out of bounds.
    let result = converter.convert_row(&batch, 2);
    assert!(result.is_err(), "out-of-bounds row must return an error");
    let msg = result.unwrap_err().to_string();
    assert!(
        msg.contains("out of bounds") || msg.contains("row_index"),
        "error message should mention bounds: {msg}"
    );
}

#[test]
fn converter_schema_json_is_valid_avro_object() {
    let schema = stock_schema();
    let converter = ArrowToAvroConverter::new(schema).unwrap();
    let json = converter.schema_json();

    // Must be a JSON object (Avro record schema).
    assert!(
        json.trim_start().starts_with('{'),
        "schema_json must be a JSON object: {json}"
    );
    // Must declare type "record".
    assert!(
        json.contains("\"record\""),
        "schema_json must contain type=record: {json}"
    );
    // Must have a fields array.
    assert!(
        json.contains("\"fields\""),
        "schema_json must contain fields array: {json}"
    );
}
