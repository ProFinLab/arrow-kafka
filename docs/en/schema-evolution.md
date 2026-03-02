# Schema Evolution Guide

This document explains how to manage Avro schema evolution when using `arrow-kafka-pyo3` with Confluent Schema Registry.

## Overview

The Confluent Schema Registry enforces compatibility rules when a schema is updated. Understanding these rules is crucial for deploying schema changes in production without breaking downstream consumers.

## Compatibility Modes

The Schema Registry supports several compatibility modes, but the most commonly used are:

| Mode | Description | Recommended for |
|------|-------------|-----------------|
| `BACKWARD` | New schema can read data written with old schema | **Default**. New consumers read old data. |
| `FORWARD` | Old schema can read data written with new schema | New producers, old consumers. |
| `FULL` | Both `BACKWARD` and `FORWARD` | Strictest. Requires full compatibility. |

By default, most Schema Registry installations use `BACKWARD` compatibility.

## Common Evolution Scenarios

### ✅ Adding a Nullable Field (Backward Compatible)

```python
# Old schema
{
  "type": "record",
  "name": "Trade",
  "fields": [
    {"name": "symbol", "type": "string"},
    {"name": "price", "type": "double"}
  ]
}

# New schema
{
  "type": "record",
  "name": "Trade",
  "fields": [
    {"name": "symbol", "type": "string"},
    {"name": "price", "type": "double"},
    {"name": "exchange", "type": ["null", "string"], "default": null}  # ✅
  ]
}
```

**Why it works:** New readers see `exchange` as nullable field with `null` default. Old readers ignore the new field.

### ❌ Adding a Non-Nullable Field (Incompatible)

```python
# Old schema
{
  "type": "record",
  "name": "Trade",
  "fields": [
    {"name": "symbol", "type": "string"},
    {"name": "price", "type": "double"}
  ]
}

# New schema - WILL FAIL
{
  "type": "record",
  "name": "Trade",
  "fields": [
    {"name": "symbol", "type": "string"},
    {"name": "price", "type": "double"},
    {"name": "exchange", "type": "string"}  # ❌ Missing default
  ]
}
```

**Solution:** Add a `default` value:

```python
{
  "type": "record",
  "name": "Trade",
  "fields": [
    {"name": "symbol", "type": "string"},
    {"name": "price", "type": "double"},
    {"name": "exchange", "type": "string", "default": "NYSE"}  # ✅ With default
  ]
}
```

### ❌ Changing Field Type (Incompatible)

Avro does **not** support type widening:

| Change | Compatible? | Notes |
|--------|-------------|-------|
| `int` → `long` | ❌ | Avro schemas are nominal, not structural |
| `float` → `double` | ❌ | |
| `string` → `bytes` | ❌ | |
| `["null", "string"]` → `string` | ❌ | Removing nullability |

**Solution:** Use a new field name or deploy a breaking change that requires all consumers to upgrade simultaneously.

### ❌ Renaming a Field (Incompatible)

```python
# Old schema
{"name": "customer_id", "type": "string"}

# New schema - WILL FAIL
{"name": "client_id", "type": "string"}  # ❌ Name changed
```

**Solution:** Use `aliases`:

```python
{
  "name": "client_id",
  "type": "string",
  "aliases": ["customer_id"]  # ✅ Old name as alias
}
```

### ❌ Removing a Field (Incompatible)

Removing a field is **never** backward compatible in `BACKWARD` mode.

**Solution:** Mark the field as deprecated but keep it in the schema.

## Materialize Compatibility

Materialize's `SOURCE … FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY` expects:

1. **Subject naming**: Default `{topic}-value` (use `subject_name_strategy="topic_name"`)
2. **Schema compatibility**: Materialize reads data using the **latest** schema registered for the subject
3. **Evolution impact**: If a new schema is incompatible, Materialize will fail to decode old messages

### Materialize Source Definition Example

```sql
CREATE SOURCE trades
FROM KAFKA BROKER 'localhost:9092'
TOPIC 'trades'
KEY FORMAT BYTES
VALUE FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'http://localhost:8081'
ENVELOPE NONE;
```

### Testing Schema Changes

Before deploying a schema change:

1. **Register the new schema in a test environment**
2. **Verify Materialize can read historical data**: Start Materialize and ensure it can decode messages written with the old schema
3. **Verify new producers can write**: Test your `arrow-kafka-pyo3` producer with the new schema
4. **Deploy in production**: Register the new schema, then deploy updated producers

## Best Practices

### 1. Always Use Nullable Fields for Optional Data

```python
# Instead of:
{"name": "optional_field", "type": "string"}

# Use:
{"name": "optional_field", "type": ["null", "string"], "default": null}
```

### 2. Plan for Evolution from Day One

Define clear ownership and process for schema changes. Consider:

- **Schema review process** for all changes
- **Version tagging** in field names (e.g., `price_v2` instead of changing `price` type)
- **Deprecation policy** for removing fields

### 3. Test with Real Data

Use the Schema Registry's compatibility API to test changes:

```bash
# Check if new schema is compatible with old
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"schema": "{\"type\":\"record\",\"name\":\"Trade\",\"fields\":[{\"name\":\"symbol\",\"type\":\"string\"}]}"}' \
  http://localhost:8081/compatibility/subjects/trades-value/versions/latest
```

### 4. Monitor Schema Changes

Track schema evolution in your observability stack:

- Alert on schema registry errors in producers
- Monitor schema ID changes in consumer metrics
- Log schema evolution events for audit trails

## Common Pitfalls

### Pitfall 1: Changing Logical Types

```python
# Changing from date to timestamp breaks compatibility
{"name": "event_date", "type": "int", "logicalType": "date"}  # ❌
{"name": "event_date", "type": "long", "logicalType": "timestamp-millis"}  # ❌
```

**Solution:** Use a new field name.

### Pitfall 2: Changing Decimal Precision/Scale

```python
# Old: decimal(10,2)
# New: decimal(12,2) - INCOMPATIBLE
```

Decimal changes require a new field.

### Pitfall 3: Changing Array/Map Element Types

```python
# Changing array element type is incompatible
{"name": "tags", "type": {"type": "array", "items": "string"}}  # ❌
{"name": "tags", "type": {"type": "array", "items": "int"}}     # ❌
```

## Emergency Rollback

If an incompatible schema is accidentally registered:

1. **Disable producers** writing with the new schema
2. **Revert to previous schema version** using Schema Registry API
3. **Restart consumers** that may have cached the bad schema
4. **Investigate root cause** and update your deployment process

## Resources

- [Confluent Schema Registry Documentation](https://docs.confluent.io/platform/current/schema-registry/index.html)
- [Avro Specification](https://avro.apache.org/docs/current/spec.html)
- [Materialize Avro Source Documentation](https://materialize.com/docs/sql/create-source/avro/)