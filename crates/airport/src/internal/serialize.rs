use crate::error::AirportError;

pub type Result<T> = std::result::Result<T, AirportError>;

/// Compresses data using ZStandard compression.
pub fn compress_catalog(data: &[u8]) -> Result<Vec<u8>> {
    zstd::encode_all(data, 3).map_err(Into::into)
}

/// Decompresses ZStandard-compressed data.
#[allow(dead_code)]
pub fn decompress_catalog(data: &[u8]) -> Result<Vec<u8>> {
    zstd::decode_all(data).map_err(Into::into)
}

/// Creates an AirportSerializedCompressedContent array [uncompressed_len, compressed_data].
/// The C++ struct uses MSGPACK_DEFINE (array format), not MAP format.
pub fn wrap_compressed_content(
    uncompressed: &[u8],
    compressed: &[u8],
) -> Result<Vec<u8>> {
    // Encode as msgpack array: [uint32(uncompressed_len), string(compressed_data)]
    let content: (u32, &[u8]) = (uncompressed.len() as u32, compressed);
    // Use rmp (low-level) to encode as array
    let mut buf = Vec::new();
    rmp::encode::write_array_len(&mut buf, 2)
        .map_err(|e| AirportError::Serialization(e.to_string()))?;
    rmp::encode::write_u32(&mut buf, uncompressed.len() as u32)
        .map_err(|e| AirportError::Serialization(e.to_string()))?;
    // Write compressed bytes as msgpack str (raw bytes, no UTF-8 validation).
    // The C++ AirportSerializedCompressedContent struct uses std::string for
    // the data field, which requires msgpack str format, not bin.
    rmp::encode::write_str_len(&mut buf, compressed.len() as u32)
        .map_err(|e| AirportError::Serialization(e.to_string()))?;
    buf.extend_from_slice(compressed);
    let _ = content;
    Ok(buf)
}
