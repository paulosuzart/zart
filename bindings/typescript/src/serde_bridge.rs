#![allow(dead_code)]
//! JSON <-> JS bridging. Plain JSON crosses the boundary via napi's `serde-json`
//! feature. Rich JS types (Date, BigInt, Uint8Array, UUID) are encoded on the TS side
//! into JSON; this module offers the Rust-side helpers for the conventions:
//! Date -> RFC3339 string, BigInt -> decimal string, bytes -> base64 string.

use base64::{Engine, engine::general_purpose::STANDARD};

use crate::errors::schema_err;

pub fn bytes_to_json(bytes: &[u8]) -> serde_json::Value {
    serde_json::Value::String(STANDARD.encode(bytes))
}

pub fn json_to_bytes(v: &serde_json::Value) -> napi::Result<Vec<u8>> {
    let s = v
        .as_str()
        .ok_or_else(|| schema_err("expected base64 string"))?;
    STANDARD.decode(s).map_err(schema_err)
}

pub fn json_to_datetime(v: &serde_json::Value) -> napi::Result<chrono::DateTime<chrono::Utc>> {
    let s = v
        .as_str()
        .ok_or_else(|| schema_err("expected ISO-8601 string"))?;
    chrono::DateTime::parse_from_rfc3339(s)
        .map(|d| d.with_timezone(&chrono::Utc))
        .map_err(schema_err)
}

pub fn datetime_to_json(d: &chrono::DateTime<chrono::Utc>) -> serde_json::Value {
    serde_json::Value::String(d.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
}

pub fn json_to_uuid(v: &serde_json::Value) -> napi::Result<uuid::Uuid> {
    let s = v
        .as_str()
        .ok_or_else(|| schema_err("expected UUID string"))?;
    uuid::Uuid::parse_str(s).map_err(schema_err)
}

/// Integers beyond JS safe range are carried as decimal strings.
pub fn json_to_i64(v: &serde_json::Value) -> napi::Result<i64> {
    match v {
        serde_json::Value::Number(n) => n.as_i64().ok_or_else(|| schema_err("not an i64")),
        serde_json::Value::String(s) => s.parse().map_err(schema_err),
        _ => Err(schema_err("expected number or numeric string")),
    }
}

pub fn i64_to_json(n: i64) -> serde_json::Value {
    if n.unsigned_abs() <= 9_007_199_254_740_991 {
        serde_json::Value::from(n)
    } else {
        serde_json::Value::String(n.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn bytes_roundtrip() {
        assert_eq!(
            json_to_bytes(&bytes_to_json(&[1, 2, 255])).unwrap(),
            vec![1, 2, 255]
        );
    }

    #[test]
    fn date_roundtrip() {
        let d = json_to_datetime(&json!("2024-01-02T03:04:05.678Z")).unwrap();
        assert_eq!(datetime_to_json(&d), json!("2024-01-02T03:04:05.678Z"));
    }

    #[test]
    fn i64_bridge() {
        assert_eq!(i64_to_json(5), json!(5));
        assert_eq!(i64_to_json(i64::MAX), json!("9223372036854775807"));
        assert_eq!(
            json_to_i64(&json!("9223372036854775807")).unwrap(),
            i64::MAX
        );
    }

    #[test]
    fn uuid_parse() {
        assert!(json_to_uuid(&json!("550e8400-e29b-41d4-a716-446655440000")).is_ok());
        assert!(json_to_uuid(&json!("nope")).is_err());
    }
}
