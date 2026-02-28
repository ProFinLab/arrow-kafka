use std::time::Duration;

use arrow::record_batch::RecordBatch;
use rdkafka::ClientConfig;
use rdkafka::error::KafkaError;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::types::RDKafkaErrorCode;

use crate::converter::ArrowToAvroConverter;
use crate::key::build_keys_for_batch;
use crate::schema_registry::SrClient;

pub struct ArrowKafkaSink {
    sr: SrClient,
    producer: FutureProducer,
}

impl ArrowKafkaSink {
    pub fn new(
        kafka_servers: String,
        schema_registry_url: String,
        _max_in_flight: usize,
        linger_ms: u32,
        batch_size: usize,
        compression_type: String,
    ) -> anyhow::Result<Self> {
        let sr = SrClient::new(schema_registry_url)?;

        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &kafka_servers)
            .set("linger.ms", linger_ms.to_string())
            .set("batch.size", batch_size.to_string())
            .set("compression.type", &compression_type)
            .set("queue.buffering.max.messages", "1000000")
            .create()?;

        Ok(Self { sr, producer })
    }

    pub fn consume_arrow(
        &self,
        batches: &[RecordBatch],
        topic: &str,
        key_cols: Option<&[String]>,
        key_separator: &str,
    ) -> anyhow::Result<usize> {
        if batches.is_empty() {
            return Ok(0);
        }

        let schema = batches[0].schema();
        let converter = ArrowToAvroConverter::new(schema)
            .map_err(|e| anyhow::anyhow!("ArrowToAvroConverter init error: {e}"))?;

        self.process_and_send(batches, topic, &converter, key_cols, key_separator)
    }

    pub fn flush(&self, timeout_ms: u64) -> anyhow::Result<usize> {
        self.producer
            .flush(Duration::from_millis(timeout_ms))
            .map_err(|e| anyhow::anyhow!("kafka flush error: {e}"))?;
        Ok(0)
    }

    pub fn close(&self) -> anyhow::Result<()> {
        Ok(())
    }

    pub fn process_and_send(
        &self,
        batches: &[RecordBatch],
        topic: &str,
        converter: &ArrowToAvroConverter,
        key_cols: Option<&[String]>,
        key_separator: &str,
    ) -> anyhow::Result<usize> {
        let mut sent_count = 0usize;

        let schema_id = self
            .sr
            .get_or_register_schema_id_for_topic_value(topic, converter.schema_json())?;

        for batch in batches {
            let keys = build_keys_for_batch(batch, key_cols.unwrap_or(&[]), key_separator)?;

            for row_index in 0..batch.num_rows() {
                let payload = converter.convert_row(batch, row_index)?;
                let framed = SrClient::confluent_frame(schema_id, &payload);

                let key_bytes_opt = keys
                    .get(row_index)
                    .and_then(|opt| opt.as_ref().map(|v| v.as_slice()));

                let mut record: FutureRecord<'_, [u8], [u8]> =
                    FutureRecord::to(topic).payload(&framed);

                if let Some(key_bytes) = key_bytes_opt {
                    record = record.key(key_bytes);
                }

                let mut record_to_send = record;
                loop {
                    match self.producer.send_result(record_to_send) {
                        Ok(_) => {
                            sent_count += 1;
                            break;
                        }
                        Err((
                            KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull),
                            returned,
                        )) => {
                            std::thread::sleep(Duration::from_millis(5));
                            record_to_send = returned;
                        }
                        Err((e, _returned)) => {
                            return Err(anyhow::anyhow!("Fatal Kafka error: {e}"));
                        }
                    }
                }
            }
        }

        Ok(sent_count)
    }
}
