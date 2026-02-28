use arrow::array::*;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use std::fmt::Write;

pub fn build_keys_for_batch(
    batch: &RecordBatch,
    key_cols: &[String],
    sep: &str,
) -> anyhow::Result<Vec<Option<Vec<u8>>>> {
    if key_cols.is_empty() {
        return Ok(vec![None; batch.num_rows()]);
    }

    let schema = batch.schema();
    let mut col_infos: Vec<(usize, DataType)> = Vec::with_capacity(key_cols.len());
    for col in key_cols {
        let idx = schema
            .index_of(col)
            .map_err(|e| anyhow::anyhow!("key column not found: {col}: {e}"))?;
        let field = schema.field(idx);
        col_infos.push((idx, field.data_type().clone()));
    }

    let num_rows = batch.num_rows();
    let mut keys = Vec::with_capacity(num_rows);
    let mut buf = String::new();

    'next_row: for row in 0..num_rows {
        let mut parts: Vec<String> = Vec::with_capacity(col_infos.len());

        for (col_idx, dt) in &col_infos {
            let arr = batch.column(*col_idx);
            match value_to_string(arr.as_ref(), dt, row) {
                Some(s) => parts.push(s),
                None => {
                    keys.push(None);
                    continue 'next_row;
                }
            }
        }

        buf.clear();
        for (i, p) in parts.iter().enumerate() {
            if i > 0 {
                buf.push_str(sep);
            }
            buf.push_str(p);
        }
        keys.push(Some(buf.as_bytes().to_vec()));
    }

    Ok(keys)
}

fn value_to_string(array: &dyn Array, data_type: &DataType, row: usize) -> Option<String> {
    if array.is_null(row) {
        return None;
    }
    let mut s = String::new();
    let ok = match data_type {
        DataType::Utf8 => array
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|a| write!(&mut s, "{}", a.value(row)).ok()),
        DataType::LargeUtf8 => array
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .map(|a| write!(&mut s, "{}", a.value(row)).ok()),
        DataType::Int8 => array
            .as_any()
            .downcast_ref::<Int8Array>()
            .map(|a| write!(&mut s, "{}", a.value(row)).ok()),
        DataType::Int16 => array
            .as_any()
            .downcast_ref::<Int16Array>()
            .map(|a| write!(&mut s, "{}", a.value(row)).ok()),
        DataType::Int32 => array
            .as_any()
            .downcast_ref::<Int32Array>()
            .map(|a| write!(&mut s, "{}", a.value(row)).ok()),
        DataType::Int64 => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|a| write!(&mut s, "{}", a.value(row)).ok()),
        DataType::UInt8 => array
            .as_any()
            .downcast_ref::<UInt8Array>()
            .map(|a| write!(&mut s, "{}", a.value(row)).ok()),
        DataType::UInt16 => array
            .as_any()
            .downcast_ref::<UInt16Array>()
            .map(|a| write!(&mut s, "{}", a.value(row)).ok()),
        DataType::UInt32 => array
            .as_any()
            .downcast_ref::<UInt32Array>()
            .map(|a| write!(&mut s, "{}", a.value(row)).ok()),
        DataType::UInt64 => array
            .as_any()
            .downcast_ref::<UInt64Array>()
            .map(|a| write!(&mut s, "{}", a.value(row)).ok()),
        DataType::Float32 => array
            .as_any()
            .downcast_ref::<Float32Array>()
            .map(|a| write!(&mut s, "{}", a.value(row)).ok()),
        DataType::Float64 => array
            .as_any()
            .downcast_ref::<Float64Array>()
            .map(|a| write!(&mut s, "{}", a.value(row)).ok()),
        DataType::Boolean => array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .map(|a| write!(&mut s, "{}", a.value(row)).ok()),
        _ => return None,
    };
    if ok.is_some() { Some(s) } else { None }
}
