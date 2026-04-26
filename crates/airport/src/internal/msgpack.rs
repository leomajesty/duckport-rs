use crate::error::AirportError;

pub type Result<T> = std::result::Result<T, AirportError>;

/// Encodes a value to MessagePack bytes.
pub fn encode<T: serde::Serialize>(value: &T) -> Result<Vec<u8>> {
    rmp_serde::to_vec_named(value).map_err(|e| AirportError::Serialization(e.to_string()))
}

/// Decodes MessagePack bytes into a value.
pub fn decode<T: serde::de::DeserializeOwned>(data: &[u8]) -> Result<T> {
    rmp_serde::from_slice(data).map_err(|e| AirportError::Serialization(e.to_string()))
}

/// Encodes a value using msgpack array format (MSGPACK_DEFINE, not MAP).
/// Used for structures like AirportSerializedCompressedContent: [length, data].
#[allow(dead_code)]
pub fn encode_array<T: serde::Serialize>(value: &T) -> Result<Vec<u8>> {
    rmp_serde::to_vec(value).map_err(|e| AirportError::Serialization(e.to_string()))
}

/// Decodes a MessagePack value into a dynamic rmpv::Value for inspection.
pub fn decode_value(data: &[u8]) -> Result<rmpv::Value> {
    rmpv::decode::read_value(&mut std::io::Cursor::new(data))
        .map_err(|e| AirportError::Serialization(e.to_string()))
}

/// Encodes a rmpv::Value to bytes.
#[allow(dead_code)]
pub fn encode_rmpv(value: &rmpv::Value) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    rmpv::encode::write_value(&mut buf, value)
        .map_err(|e| AirportError::Serialization(e.to_string()))?;
    Ok(buf)
}

/// Decodes function parameters from msgpack-encoded app_metadata bytes.
///
/// DuckDB encodes table function parameters in a msgpack map:
///   `{ "parameters": <bytes_or_array> }`
///
/// The `parameters` value may be:
/// 1. A msgpack **string** containing Arrow IPC RecordBatch bytes (most common) –
///    each column holds one parameter value at row 0.
/// 2. A msgpack **binary** containing a msgpack-encoded array of parameters.
/// 3. A msgpack **array** directly.
///
/// Returns a Vec of JSON values representing the parameters.
pub fn decode_function_params(data: &[u8]) -> Vec<serde_json::Value> {
    if data.is_empty() {
        return vec![];
    }

    // Try to decode as a msgpack map and look for a "parameters" field.
    if let Ok(rmpv::Value::Map(map)) = decode_value(data) {
        for (k, v) in &map {
            let key = match k {
                rmpv::Value::String(s) => s.as_str().unwrap_or(""),
                _ => continue,
            };
            if key == "parameters" {
                return decode_parameters_value(v);
            }
        }
        // Map found but no "parameters" key.
        return vec![];
    }

    // Fallback: treat raw data as direct msgpack array, or Arrow IPC bytes.
    match decode_value(data) {
        Ok(rmpv::Value::Array(items)) => items.into_iter().map(rmpv_to_json).collect(),
        _ => decode_params_from_ipc_bytes(data),
    }
}

/// Decodes the value of the `parameters` msgpack field into a JSON array.
fn decode_parameters_value(v: &rmpv::Value) -> Vec<serde_json::Value> {
    match v {
        // Arrow IPC RecordBatch encoded as msgpack string (raw bytes).
        rmpv::Value::String(s) => decode_params_from_ipc_bytes(s.as_bytes()),
        // msgpack binary: try as msgpack array first, then Arrow IPC.
        rmpv::Value::Binary(b) => {
            if let Ok(rmpv::Value::Array(items)) = decode_value(b) {
                return items.into_iter().map(rmpv_to_json).collect();
            }
            decode_params_from_ipc_bytes(b)
        }
        // Direct array.
        rmpv::Value::Array(items) => items.iter().cloned().map(rmpv_to_json).collect(),
        rmpv::Value::Nil => vec![],
        _ => vec![],
    }
}

/// Decodes Arrow IPC RecordBatch bytes and extracts one value per column (row 0).
fn decode_params_from_ipc_bytes(bytes: &[u8]) -> Vec<serde_json::Value> {
    use arrow_array::cast::AsArray;
    use arrow_ipc::reader::FileReader;

    if bytes.is_empty() {
        return vec![];
    }

    // Try IPC stream reader first (most common for in-flight data).
    let mut cursor = std::io::Cursor::new(bytes);
    if let Ok(mut reader) = arrow_ipc::reader::StreamReader::try_new(&mut cursor, None) {
        if let Some(Ok(batch)) = reader.next() {
            return extract_params_from_batch(&batch);
        }
    }

    // Fallback: try IPC file reader.
    let cursor = std::io::Cursor::new(bytes);
    if let Ok(mut reader) = FileReader::try_new(cursor, None) {
        if let Some(Ok(batch)) = reader.next() {
            return extract_params_from_batch(&batch);
        }
    }

    vec![]
}

/// Extracts one JSON value per column from row 0 of a RecordBatch.
fn extract_params_from_batch(batch: &arrow_array::RecordBatch) -> Vec<serde_json::Value> {
    use arrow_array::{Array, BooleanArray, Date32Array, Float32Array, Float64Array,
                      Int8Array, Int16Array, Int32Array, Int64Array, LargeStringArray, StringArray,
                      TimestampMicrosecondArray};

    (0..batch.num_columns())
        .map(|i| {
            let col = batch.column(i);
            if col.is_empty() || col.is_null(0) {
                return serde_json::Value::Null;
            }
            if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
                return serde_json::json!(a.value(0));
            }
            if let Some(a) = col.as_any().downcast_ref::<Int32Array>() {
                return serde_json::json!(a.value(0));
            }
            if let Some(a) = col.as_any().downcast_ref::<Int16Array>() {
                return serde_json::json!(a.value(0));
            }
            if let Some(a) = col.as_any().downcast_ref::<Int8Array>() {
                return serde_json::json!(a.value(0));
            }
            if let Some(a) = col.as_any().downcast_ref::<Float64Array>() {
                return serde_json::json!(a.value(0));
            }
            if let Some(a) = col.as_any().downcast_ref::<Float32Array>() {
                return serde_json::json!(a.value(0) as f64);
            }
            if let Some(a) = col.as_any().downcast_ref::<BooleanArray>() {
                return serde_json::json!(a.value(0));
            }
            if let Some(a) = col.as_any().downcast_ref::<StringArray>() {
                return serde_json::json!(a.value(0));
            }
            if let Some(a) = col.as_any().downcast_ref::<LargeStringArray>() {
                return serde_json::json!(a.value(0));
            }
            if let Some(a) = col.as_any().downcast_ref::<TimestampMicrosecondArray>() {
                return serde_json::json!(a.value(0));
            }
            if let Some(a) = col.as_any().downcast_ref::<Date32Array>() {
                return serde_json::json!(a.value(0));
            }
            serde_json::Value::Null
        })
        .collect()
}

/// Converts a rmpv::Value to serde_json::Value.
pub fn rmpv_to_json(v: rmpv::Value) -> serde_json::Value {
    match v {
        rmpv::Value::Nil => serde_json::Value::Null,
        rmpv::Value::Boolean(b) => serde_json::Value::Bool(b),
        rmpv::Value::Integer(i) => {
            if let Some(n) = i.as_i64() {
                serde_json::Value::Number(n.into())
            } else if let Some(n) = i.as_u64() {
                serde_json::Value::Number(n.into())
            } else {
                serde_json::Value::Null
            }
        }
        rmpv::Value::F32(f) => serde_json::json!(f),
        rmpv::Value::F64(f) => serde_json::json!(f),
        rmpv::Value::String(s) => {
            serde_json::Value::String(s.into_str().unwrap_or_default())
        }
        rmpv::Value::Binary(b) => {
            serde_json::Value::String(hex::encode(b))
        }
        rmpv::Value::Array(arr) => {
            serde_json::Value::Array(arr.into_iter().map(rmpv_to_json).collect())
        }
        rmpv::Value::Map(m) => {
            let obj: serde_json::Map<_, _> = m
                .into_iter()
                .filter_map(|(k, v)| {
                    if let rmpv::Value::String(key) = k {
                        Some((key.into_str().unwrap_or_default(), rmpv_to_json(v)))
                    } else {
                        None
                    }
                })
                .collect();
            serde_json::Value::Object(obj)
        }
        rmpv::Value::Ext(_, _) => serde_json::Value::Null,
    }
}
