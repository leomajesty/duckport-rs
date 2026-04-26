/// Geometry metadata support for Arrow schemas.
///
/// DuckDB Airport Extension uses specific Arrow metadata keys to identify geometry columns.
/// This module provides helpers for working with geometry column metadata.

use arrow_schema::Field;

/// Metadata key indicating a column contains geometry data.
pub const ARROW_EXTENSION_NAME_KEY: &str = "ARROW:extension:name";
/// Metadata value for WKB-encoded geometry.
pub const GEOARROW_WKB: &str = "geoarrow.wkb";
/// Metadata key for geometry CRS.
pub const ARROW_EXTENSION_METADATA_KEY: &str = "ARROW:extension:metadata";

/// Returns true if a field represents a geometry column.
pub fn is_geometry_field(field: &Field) -> bool {
    field
        .metadata()
        .get(ARROW_EXTENSION_NAME_KEY)
        .map(|v| v == GEOARROW_WKB)
        .unwrap_or(false)
}

/// Creates a geometry field with appropriate Arrow metadata.
pub fn geometry_field(name: &str, nullable: bool) -> Field {
    let mut metadata = std::collections::HashMap::new();
    metadata.insert(ARROW_EXTENSION_NAME_KEY.to_string(), GEOARROW_WKB.to_string());
    Field::new(name, arrow_schema::DataType::LargeBinary, nullable)
        .with_metadata(metadata)
}

/// Creates a geometry field with CRS metadata.
pub fn geometry_field_with_crs(name: &str, nullable: bool, crs: &str) -> Field {
    let mut metadata = std::collections::HashMap::new();
    metadata.insert(ARROW_EXTENSION_NAME_KEY.to_string(), GEOARROW_WKB.to_string());
    metadata.insert(ARROW_EXTENSION_METADATA_KEY.to_string(), crs.to_string());
    Field::new(name, arrow_schema::DataType::LargeBinary, nullable)
        .with_metadata(metadata)
}

/// Encodes geometry to WKB bytes.
/// This is a placeholder - actual WKB encoding depends on the geometry library used.
pub fn encode_wkb(wkb: Vec<u8>) -> Vec<u8> {
    wkb
}

/// Decodes WKB bytes to a geometry.
/// Returns the raw WKB bytes for downstream use.
pub fn decode_wkb(data: &[u8]) -> Vec<u8> {
    data.to_vec()
}
