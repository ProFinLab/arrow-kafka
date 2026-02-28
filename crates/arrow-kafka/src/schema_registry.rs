use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use schema_registry_converter::blocking::schema_registry;
use schema_registry_converter::blocking::schema_registry::SrSettings;
use schema_registry_converter::schema_registry_common::{SchemaType, SuppliedSchema};

#[derive(Clone)]
pub struct SrClient {
    settings: Arc<SrSettings>,
    cache: Arc<Mutex<HashMap<String, u32>>>,
}

impl SrClient {
    pub fn new(schema_registry_url: String) -> anyhow::Result<Self> {
        let settings = SrSettings::new(schema_registry_url);
        Ok(Self {
            settings: Arc::new(settings),
            cache: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub fn subject_for_topic_value(topic: &str) -> String {
        format!("{topic}-value")
    }

    pub fn confluent_frame(schema_id: u32, avro_payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(1 + 4 + avro_payload.len());
        out.push(0u8);
        out.extend_from_slice(&schema_id.to_be_bytes());
        out.extend_from_slice(avro_payload);
        out
    }

    pub fn get_or_register_schema_id_for_topic_value(
        &self,
        topic: &str,
        schema_json: &str,
    ) -> anyhow::Result<u32> {
        let subject = Self::subject_for_topic_value(topic);
        let cache_key = format!("{subject}::{schema_json}");
        if let Some(id) = self
            .cache
            .lock()
            .ok()
            .and_then(|m| m.get(&cache_key).copied())
        {
            return Ok(id);
        }

        let supplied = SuppliedSchema {
            name: None,
            schema_type: SchemaType::Avro,
            schema: schema_json.to_string(),
            references: vec![],
            properties: None,
            tags: None,
        };

        let registered = schema_registry::post_schema(&self.settings, subject.clone(), supplied)
            .map_err(|e| anyhow::anyhow!("schema registry error: {e}"))?;

        let schema_id = registered.id;
        if let Ok(mut m) = self.cache.lock() {
            m.insert(cache_key, schema_id);
        }

        Ok(schema_id)
    }
}
