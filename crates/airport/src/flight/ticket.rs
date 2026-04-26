use crate::catalog::ScanOptions;
use crate::error::AirportError;
use serde::{Deserialize, Serialize};

pub type Result<T> = std::result::Result<T, AirportError>;

/// TicketData represents the decoded content of a Flight ticket.
/// Tickets are JSON-encoded for simplicity and transparency.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TicketData {
    /// Catalog name (optional, defaults to default catalog).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub catalog: String,

    /// Schema name.
    pub schema: String,

    /// Table name. Either table or table_function must be set, not both.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub table: String,

    /// Table function name. Either table or table_function must be set.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub table_function: String,

    /// Function parameters (optional, only valid when table_function is set).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub function_params: Option<Vec<u8>>,

    /// Time point unit for time-travel queries (optional).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub time_point_unit: String,

    /// Time point value for time-travel queries (optional).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub time_point_value: String,

    /// Columns to project (optional, empty means all columns).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub columns: Vec<String>,

    /// Filters to apply (optional).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filters: Option<Vec<u8>>,
}

impl TicketData {
    /// Creates an opaque ticket from catalog, schema, and table names.
    pub fn encode_table_ticket(catalog: &str, schema: &str, table: &str) -> Result<Vec<u8>> {
        if schema.is_empty() {
            return Err(AirportError::InvalidParameters(
                "schema name cannot be empty".to_string(),
            ));
        }
        if table.is_empty() {
            return Err(AirportError::InvalidParameters(
                "table name cannot be empty".to_string(),
            ));
        }
        let ticket = TicketData {
            schema: schema.to_string(),
            table: table.to_string(),
            ..Default::default()
        };
        ticket.encode()
    }

    /// Serializes TicketData to JSON bytes.
    pub fn encode(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(self).map_err(Into::into)
    }

    /// Parses ticket bytes into TicketData.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.is_empty() {
            return Err(AirportError::InvalidParameters(
                "ticket cannot be empty".to_string(),
            ));
        }
        let ticket: TicketData = serde_json::from_slice(bytes)?;

        if ticket.schema.is_empty() {
            return Err(AirportError::InvalidParameters(
                "decoded ticket has empty schema name".to_string(),
            ));
        }
        if ticket.table.is_empty() && ticket.table_function.is_empty() {
            return Err(AirportError::InvalidParameters(
                "ticket must have either table or table_function set".to_string(),
            ));
        }
        if !ticket.table.is_empty() && !ticket.table_function.is_empty() {
            return Err(AirportError::InvalidParameters(
                "ticket cannot have both table and table_function set".to_string(),
            ));
        }
        if ticket.function_params.is_some() && ticket.table_function.is_empty() {
            return Err(AirportError::InvalidParameters(
                "function_params only valid with table_function".to_string(),
            ));
        }
        if !ticket.time_point_unit.is_empty() && ticket.time_point_value.is_empty() {
            return Err(AirportError::InvalidParameters(
                "time_point_value must be set when time_point_unit is specified".to_string(),
            ));
        }
        if !ticket.time_point_value.is_empty() && ticket.time_point_unit.is_empty() {
            return Err(AirportError::InvalidParameters(
                "time_point_unit must be set when time_point_value is specified".to_string(),
            ));
        }
        Ok(ticket)
    }

    /// Converts TicketData to ScanOptions.
    pub fn to_scan_options(&self) -> ScanOptions {
        let time_point = if !self.time_point_unit.is_empty() && !self.time_point_value.is_empty() {
            Some(crate::catalog::TimePoint {
                unit: self.time_point_unit.clone(),
                value: self.time_point_value.clone(),
            })
        } else {
            None
        };

        ScanOptions {
            columns: self.columns.clone(),
            filter: self.filters.clone(),
            time_point,
            ..Default::default()
        }
    }
}
