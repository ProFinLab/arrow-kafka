use arrow::array::{Float64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

#[test]
fn converter_row_roundtrip_basic_types() {
    use arrow_kafka::ArrowToAvroConverter;
    let schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
    ]));

    let symbol = StringArray::from(vec!["A", "B"]);
    let price = Float64Array::from(vec![1.0_f64, 2.5_f64]);

    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(symbol), Arc::new(price)]).unwrap();

    let converter = ArrowToAvroConverter::new(schema).unwrap();

    let row0 = converter.convert_row(&batch, 0).unwrap();
    let row1 = converter.convert_row(&batch, 1).unwrap();

    assert!(!row0.is_empty());
    assert!(!row1.is_empty());
    assert_ne!(row0, row1);
}

#[test]
fn key_build_single_and_composite() {
    use arrow::array::Int64Array;
    use arrow_kafka::key::build_keys_for_batch;

    let schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("trade_date", DataType::Int64, false),
        Field::new("close", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["000001.SZ", "000002.SZ"])),
            Arc::new(Int64Array::from(vec![20240101_i64, 20240101_i64])),
            Arc::new(Float64Array::from(vec![10.5, 20.3])),
        ],
    )
    .unwrap();

    let keys = build_keys_for_batch(&batch, &[], "_").unwrap();
    assert_eq!(keys.len(), 2);
    assert!(keys[0].is_none());
    assert!(keys[1].is_none());

    let keys = build_keys_for_batch(&batch, &["symbol".to_string()], "_").unwrap();
    assert_eq!(keys.len(), 2);
    assert_eq!(keys[0].as_deref(), Some(b"000001.SZ".as_slice()));
    assert_eq!(keys[1].as_deref(), Some(b"000002.SZ".as_slice()));

    let keys = build_keys_for_batch(
        &batch,
        &["symbol".to_string(), "trade_date".to_string()],
        "_",
    )
    .unwrap();
    assert_eq!(keys.len(), 2);
    assert_eq!(keys[0].as_deref(), Some(b"000001.SZ_20240101".as_slice()));
    assert_eq!(keys[1].as_deref(), Some(b"000002.SZ_20240101".as_slice()));
}

#[test]
fn key_build_null_returns_none() {
    use arrow::array::Int64Array;
    use arrow_kafka::key::build_keys_for_batch;

    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, true),
        Field::new("b", DataType::Int64, true),
    ]));
    let a = StringArray::from(vec![Some("x"), None, Some("z")]);
    let b = Int64Array::from(vec![Some(1_i64), Some(2), None]);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(a), Arc::new(b)]).unwrap();

    let keys = build_keys_for_batch(&batch, &["a".to_string(), "b".to_string()], "_").unwrap();
    assert_eq!(keys.len(), 3);
    assert_eq!(keys[0].as_deref(), Some(b"x_1".as_slice()));
    assert!(keys[1].is_none());
    assert!(keys[2].is_none());
}
