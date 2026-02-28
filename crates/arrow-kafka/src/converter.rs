use anyhow::{Context, Result};
use apache_avro::{Schema as AvroSchemaDef, types::Value as AvroValue};
use arrow::array::*;
use arrow::datatypes::{DataType, Field, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow_avro::schema::AvroSchema;
use std::convert::TryFrom;

pub struct ArrowToAvroConverter {
    avro_schema: AvroSchemaDef,
    arrow_schema: SchemaRef,
    schema_json: String,
}

impl ArrowToAvroConverter {
    pub fn new(arrow_schema: SchemaRef) -> Result<Self> {
        let avro_schema_wrapper = AvroSchema::try_from(arrow_schema.as_ref())
            .map_err(|e| anyhow::anyhow!("Arrow to Avro conversion failed: {}", e))?;

        let avro_schema = AvroSchemaDef::parse_str(&avro_schema_wrapper.json_string)
            .context("Failed to parse the generated Avro JSON schema")?;

        Ok(Self {
            avro_schema,
            arrow_schema,
            schema_json: avro_schema_wrapper.json_string,
        })
    }

    pub fn schema_json(&self) -> &str {
        &self.schema_json
    }

    pub fn convert_row(&self, batch: &RecordBatch, row_index: usize) -> Result<Vec<u8>> {
        if row_index >= batch.num_rows() {
            return Err(anyhow::anyhow!(
                "row_index {} out of bounds for batch with {} rows",
                row_index,
                batch.num_rows()
            ));
        }

        let mut fields = Vec::with_capacity(batch.num_columns());

        for (col_idx, field) in self.arrow_schema.fields().iter().enumerate() {
            let column = batch.column(col_idx);
            let value = arrow_value_to_avro_value(column.as_ref(), field, row_index)?;
            fields.push((field.name().clone(), value));
        }

        let record = AvroValue::Record(fields);
        let buffer = apache_avro::to_avro_datum(&self.avro_schema, record)
            .context("Failed to encode pure Avro datum")?;

        Ok(buffer)
    }
}

fn arrow_value_to_avro_value(
    array: &dyn Array,
    field: &Field,
    row_index: usize,
) -> Result<AvroValue> {
    let nullable = field.is_nullable();

    if array.is_null(row_index) {
        return if nullable {
            Ok(AvroValue::Union(0, Box::new(AvroValue::Null)))
        } else {
            Ok(AvroValue::Null)
        };
    }

    let raw = match field.data_type() {
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    anyhow::anyhow!("Expected StringArray for field {}", field.name())
                })?;
            AvroValue::String(arr.value(row_index).to_string())
        }
        DataType::LargeUtf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| {
                    anyhow::anyhow!("Expected LargeStringArray for field {}", field.name())
                })?;
            AvroValue::String(arr.value(row_index).to_string())
        }
        DataType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>().ok_or_else(|| {
                anyhow::anyhow!("Expected Int8Array for field {}", field.name())
            })?;
            AvroValue::Int(arr.value(row_index) as i32)
        }
        DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().ok_or_else(|| {
                anyhow::anyhow!("Expected Int16Array for field {}", field.name())
            })?;
            AvroValue::Int(arr.value(row_index) as i32)
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                anyhow::anyhow!("Expected Int32Array for field {}", field.name())
            })?;
            AvroValue::Int(arr.value(row_index))
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                anyhow::anyhow!("Expected Int64Array for field {}", field.name())
            })?;
            AvroValue::Long(arr.value(row_index))
        }
        DataType::UInt8 => {
            let arr = array.as_any().downcast_ref::<UInt8Array>().ok_or_else(|| {
                anyhow::anyhow!("Expected UInt8Array for field {}", field.name())
            })?;
            AvroValue::Long(arr.value(row_index) as i64)
        }
        DataType::UInt16 => {
            let arr = array
                .as_any()
                .downcast_ref::<UInt16Array>()
                .ok_or_else(|| {
                    anyhow::anyhow!("Expected UInt16Array for field {}", field.name())
                })?;
            AvroValue::Long(arr.value(row_index) as i64)
        }
        DataType::UInt32 => {
            let arr = array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| {
                    anyhow::anyhow!("Expected UInt32Array for field {}", field.name())
                })?;
            AvroValue::Long(arr.value(row_index) as i64)
        }
        DataType::UInt64 => {
            let arr = array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| {
                    anyhow::anyhow!("Expected UInt64Array for field {}", field.name())
                })?;
            AvroValue::Long(arr.value(row_index) as i64)
        }
        DataType::Float32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| {
                    anyhow::anyhow!("Expected Float32Array for field {}", field.name())
                })?;
            AvroValue::Float(arr.value(row_index))
        }
        DataType::Float64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    anyhow::anyhow!("Expected Float64Array for field {}", field.name())
                })?;
            AvroValue::Double(arr.value(row_index))
        }
        DataType::Boolean => {
            let arr = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    anyhow::anyhow!("Expected BooleanArray for field {}", field.name())
                })?;
            AvroValue::Boolean(arr.value(row_index))
        }
        dt => {
            return Err(anyhow::anyhow!(
                "Unsupported Arrow data type {:?} for field {} in Avro conversion",
                dt,
                field.name()
            ));
        }
    };

    if nullable {
        Ok(AvroValue::Union(1, Box::new(raw)))
    } else {
        Ok(raw)
    }
}
