use arrow_schema::Schema as ArrowSchema;
use std::sync::Arc;

/// ScanOptions provides options for table scans.
#[derive(Debug, Clone, Default)]
pub struct ScanOptions {
    /// Columns to return. If empty, all columns are projected.
    /// The server may use this information for optimization.
    pub columns: Vec<String>,

    /// Filter contains a serialized JSON predicate expression from DuckDB.
    /// If None, no filtering (return all rows).
    pub filter: Option<Vec<u8>>,

    /// Limit is maximum rows to return.
    /// If 0 or negative, no limit.
    pub limit: i64,

    /// BatchSize is hint for RecordReader batch size.
    /// If 0, implementation chooses default.
    pub batch_size: usize,

    /// TimePoint specifies point-in-time for time-travel queries.
    /// None for "current" time (no time travel).
    pub time_point: Option<TimePoint>,
}

/// TimePoint represents a point-in-time for time-travel queries.
#[derive(Debug, Clone)]
pub struct TimePoint {
    /// Unit specifies time granularity ("timestamp", "version", "snapshot").
    pub unit: String,
    /// Value is the time point value (format depends on Unit).
    pub value: String,
}

/// SchemaRequest contains parameters for determining dynamic schema.
#[derive(Debug, Clone, Default)]
pub struct SchemaRequest {
    /// Parameters for table functions (decoded values).
    pub parameters: Vec<serde_json::Value>,
    /// TimePoint for time-travel queries.
    pub time_point: Option<TimePoint>,
    /// Columns requested (for projection pushdown).
    pub columns: Vec<String>,
}

/// FunctionSignature describes scalar/table function types.
#[derive(Debug, Clone)]
pub struct FunctionSignature {
    /// Parameters is list of parameter types (in order).
    pub parameters: Vec<arrow_schema::DataType>,
    /// ReturnType is the function's return type (for scalar functions).
    /// None for table functions.
    pub return_type: Option<arrow_schema::DataType>,
    /// Variadic indicates if last parameter accepts multiple values.
    pub variadic: bool,
}

/// DMLOptions carries options for DML operations (INSERT, UPDATE, DELETE).
#[derive(Debug, Clone, Default)]
pub struct DMLOptions {
    /// Returning indicates whether a RETURNING clause was specified.
    pub returning: bool,
    /// ReturningColumns specifies which columns to include in RETURNING results.
    pub returning_columns: Vec<String>,
}

/// DMLResult holds the outcome of INSERT, UPDATE, or DELETE operations.
pub struct DMLResult {
    /// AffectedRows is the count of rows affected.
    pub affected_rows: i64,
    /// ReturningData contains rows affected by the operation when
    /// a RETURNING clause was specified.
    pub returning_data: Option<Box<dyn arrow_array::RecordBatchReader + Send>>,
}

/// ColumnStats contains statistics for a single table column.
#[derive(Debug, Clone, Default)]
pub struct ColumnStats {
    pub has_not_null: Option<bool>,
    pub has_null: Option<bool>,
    pub distinct_count: Option<u64>,
    pub min: Option<serde_json::Value>,
    pub max: Option<serde_json::Value>,
    pub max_string_length: Option<u64>,
    pub contains_unicode: Option<bool>,
}

/// CatalogVersion holds catalog version information.
#[derive(Debug, Clone)]
pub struct CatalogVersion {
    pub version: u64,
    pub is_fixed: bool,
}

/// ProjectSchema returns a projected schema containing only the specified columns.
pub fn project_schema(
    schema: &ArrowSchema,
    columns: &[&str],
) -> Arc<ArrowSchema> {
    if columns.is_empty() {
        return Arc::new(schema.clone());
    }
    let col_index: std::collections::HashMap<&str, usize> = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| (f.name().as_str(), i))
        .collect();

    let fields: Vec<_> = columns
        .iter()
        .filter_map(|col| col_index.get(*col).map(|&i| schema.field(i).clone()))
        .collect();

    if fields.is_empty() {
        return Arc::new(schema.clone());
    }

    Arc::new(ArrowSchema::new_with_metadata(
        fields,
        schema.metadata().clone(),
    ))
}

/// Conflict resolution options for DDL operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OnConflict {
    /// Return error if object already exists.
    #[default]
    Error,
    /// Silently ignore if object already exists.
    Ignore,
    /// Replace existing object.
    Replace,
}

/// Options for CREATE SCHEMA.
#[derive(Debug, Clone, Default)]
pub struct CreateSchemaOptions {
    pub on_conflict: OnConflict,
}

/// Options for DROP SCHEMA.
#[derive(Debug, Clone, Default)]
pub struct DropSchemaOptions {
    pub ignore_not_found: bool,
    pub cascade: bool,
}

/// Options for CREATE TABLE.
#[derive(Debug, Clone, Default)]
pub struct CreateTableOptions {
    pub on_conflict: OnConflict,
    pub comment: Option<String>,
}

/// Options for DROP TABLE.
#[derive(Debug, Clone, Default)]
pub struct DropTableOptions {
    pub ignore_not_found: bool,
}

/// Options for ADD COLUMN.
#[derive(Debug, Clone, Default)]
pub struct AddColumnOptions {
    pub ignore_if_exists: bool,
}

/// Options for REMOVE COLUMN.
#[derive(Debug, Clone, Default)]
pub struct RemoveColumnOptions {
    pub ignore_not_found: bool,
}

/// Options for RENAME COLUMN.
#[derive(Debug, Clone, Default)]
pub struct RenameColumnOptions {
    pub ignore_not_found: bool,
}

/// Options for RENAME TABLE.
#[derive(Debug, Clone, Default)]
pub struct RenameTableOptions {
    pub ignore_not_found: bool,
}

/// Options for CHANGE COLUMN TYPE.
#[derive(Debug, Clone, Default)]
pub struct ChangeColumnTypeOptions {
    pub ignore_not_found: bool,
}

/// Options for SET NOT NULL.
#[derive(Debug, Clone, Default)]
pub struct SetNotNullOptions {
    pub ignore_not_found: bool,
}

/// Options for DROP NOT NULL.
#[derive(Debug, Clone, Default)]
pub struct DropNotNullOptions {
    pub ignore_not_found: bool,
}

/// Options for SET DEFAULT.
#[derive(Debug, Clone, Default)]
pub struct SetDefaultOptions {
    pub ignore_not_found: bool,
}

/// Options for ADD FIELD (struct fields).
#[derive(Debug, Clone, Default)]
pub struct AddFieldOptions {
    pub ignore_if_exists: bool,
}

/// Options for RENAME FIELD.
#[derive(Debug, Clone, Default)]
pub struct RenameFieldOptions {
    pub ignore_not_found: bool,
}

/// Options for REMOVE FIELD.
#[derive(Debug, Clone, Default)]
pub struct RemoveFieldOptions {
    pub ignore_not_found: bool,
}
