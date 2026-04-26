pub mod parse;
pub mod types;

pub use parse::parse;
pub use types::*;

/// Filters represents serialized DuckDB filter expressions.
pub struct Filters {
    raw: Vec<u8>,
}

impl Filters {
    pub fn new(raw: Vec<u8>) -> Self {
        Filters { raw }
    }

    /// Returns the raw JSON bytes.
    pub fn raw(&self) -> &[u8] {
        &self.raw
    }

    /// Parses the filter into a structured FilterPushdown.
    pub fn parse(&self) -> Option<FilterPushdown> {
        parse::parse(&self.raw)
    }
}
