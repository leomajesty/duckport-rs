//! Unified Airport Flight server for airport-rs.
//!
//! A single server that covers all six feature areas from the airport-go examples
//! and passes the smoke tests in smoke-test/test_basic.py.
//!
//! Features:
//!   1. Basic    – in-memory `users` table (id BIGINT, name VARCHAR), 3 seed rows
//!   2. DDL      – CREATE TABLE / DROP TABLE on the `app` schema
//!   3. DML      – INSERT / UPDATE / DELETE with rowid tracking
//!   4. Filter   – parses and logs DuckDB filter JSON (server returns all rows;
//!                 DuckDB applies the filter locally)
//!   5. Functions – MULTIPLY scalar function, GENERATE_SERIES table function
//!   6. TableRef  – `series_ref` delegating to DuckDB's generate_series
//!
//! Default port: 50052. Override with --port <N>.
//!
//! Run:
//!   cargo run --example unified
//!
//! Connect from DuckDB:
//!   INSTALL airport FROM community;
//!   LOAD airport;
//!   ATTACH 'grpc://localhost:50052' AS flight (TYPE AIRPORT);
//!   SELECT * FROM flight.app.users;

use airport::catalog::transaction::{TransactionManager, TransactionState};
use airport::catalog::dynamic::{DynamicCatalog, DynamicSchema};
use airport::catalog::function::{ScalarFunction, TableFunction};
use airport::catalog::table::{
    find_row_id_column, DeletableBatchTable, DynamicTable, InsertableTable,
    SendableRecordBatchStream, Table, UpdatableBatchTable,
};
use airport::catalog::tableref::{
    FunctionCall, FunctionCallArg, FunctionCallRequest, FunctionCallValue, TableRef,
};
use airport::catalog::types::{
    project_schema, AddColumnOptions, AddFieldOptions, CatalogVersion, ChangeColumnTypeOptions,
    CreateSchemaOptions, CreateTableOptions, DMLOptions, DMLResult, DropNotNullOptions,
    DropSchemaOptions, DropTableOptions, FunctionSignature, OnConflict, RemoveColumnOptions,
    RemoveFieldOptions, RenameColumnOptions, RenameFieldOptions, RenameTableOptions, ScanOptions,
    SetDefaultOptions, SetNotNullOptions,
};
use airport::catalog::{Catalog, Result, Schema};
use airport::config::ServerConfig;
use airport::error::AirportError;
use airport::flight::context::RequestContext;
use airport::server::new_server;
use arrow_array::{
    Array, ArrayRef, BooleanArray, Date32Array, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, LargeStringArray, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use arrow_schema::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use futures::TryStreamExt;
use std::collections::{HashMap, HashSet};
use std::env;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock, Weak};

// ─────────────────────────────────────────────────────────────────────────────
// Shared helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Clone all fields from a schema into a Vec<Field>.
/// arrow-schema 53 stores fields as Arc<Field>, so we must deref before cloning.
fn clone_fields(schema: &ArrowSchema) -> Vec<Field> {
    schema.fields().iter().map(|f| f.as_ref().clone()).collect()
}

// ─────────────────────────────────────────────────────────────────────────────
// Shared types
// ─────────────────────────────────────────────────────────────────────────────

type VersionCounter = Arc<AtomicU64>;

/// Build the rowid Field with the is_rowid metadata marker.
fn rowid_field() -> Field {
    Field::new("rowid", DataType::Int64, false).with_metadata(
        [("is_rowid".to_string(), "1".to_string())]
            .into_iter()
            .collect(),
    )
}

// ─────────────────────────────────────────────────────────────────────────────
// Value extraction helpers
// ─────────────────────────────────────────────────────────────────────────────

fn extract_i64(arr: &dyn arrow_array::Array, idx: usize) -> Option<i64> {
    if arr.is_null(idx) {
        return None;
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
        return Some(a.value(idx));
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int32Array>() {
        return Some(i64::from(a.value(idx)));
    }
    None
}

fn extract_str(arr: &dyn arrow_array::Array, idx: usize) -> Option<String> {
    if arr.is_null(idx) {
        return None;
    }
    if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
        return Some(a.value(idx).to_string());
    }
    if let Some(a) = arr.as_any().downcast_ref::<LargeStringArray>() {
        return Some(a.value(idx).to_string());
    }
    None
}

// ─────────────────────────────────────────────────────────────────────────────
// DynVal – generic value type for dynamic tables
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
enum DynVal {
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    Bool(bool),
    Str(String),
    /// Raw microseconds since Unix epoch (Arrow Timestamp(Microsecond, _))
    TimestampUs(i64),
    /// Days since Unix epoch (Arrow Date32)
    Date32(i32),
}

fn extract_dynval(arr: &dyn arrow_array::Array, idx: usize) -> Option<DynVal> {
    if arr.is_null(idx) {
        return None;
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
        return Some(DynVal::I64(a.value(idx)));
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int32Array>() {
        return Some(DynVal::I32(a.value(idx)));
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int16Array>() {
        return Some(DynVal::I64(i64::from(a.value(idx))));
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int8Array>() {
        return Some(DynVal::I64(i64::from(a.value(idx))));
    }
    if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
        return Some(DynVal::F64(a.value(idx)));
    }
    if let Some(a) = arr.as_any().downcast_ref::<Float32Array>() {
        return Some(DynVal::F32(a.value(idx)));
    }
    if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
        return Some(DynVal::Str(a.value(idx).to_string()));
    }
    if let Some(a) = arr.as_any().downcast_ref::<LargeStringArray>() {
        return Some(DynVal::Str(a.value(idx).to_string()));
    }
    if let Some(a) = arr.as_any().downcast_ref::<BooleanArray>() {
        return Some(DynVal::Bool(a.value(idx)));
    }
    // Timestamp: store raw microseconds since epoch regardless of timezone.
    if let Some(a) = arr.as_any().downcast_ref::<TimestampMicrosecondArray>() {
        return Some(DynVal::TimestampUs(a.value(idx)));
    }
    // Date32: store raw days since epoch.
    if let Some(a) = arr.as_any().downcast_ref::<Date32Array>() {
        return Some(DynVal::Date32(a.value(idx)));
    }
    None
}

fn build_column_array(field: &Field, col: &[Option<DynVal>]) -> ArrayRef {
    match field.data_type() {
        DataType::Int8 => {
            let v: Vec<Option<i8>> = col
                .iter()
                .map(|x| match x {
                    Some(DynVal::I64(n)) => Some(*n as i8),
                    Some(DynVal::I32(n)) => Some(*n as i8),
                    _ => None,
                })
                .collect();
            Arc::new(Int8Array::from(v))
        }
        DataType::Int16 => {
            let v: Vec<Option<i16>> = col
                .iter()
                .map(|x| match x {
                    Some(DynVal::I64(n)) => Some(*n as i16),
                    Some(DynVal::I32(n)) => Some(*n as i16),
                    _ => None,
                })
                .collect();
            Arc::new(Int16Array::from(v))
        }
        DataType::Int32 => {
            let v: Vec<Option<i32>> = col
                .iter()
                .map(|x| match x {
                    Some(DynVal::I32(n)) => Some(*n),
                    Some(DynVal::I64(n)) => Some(*n as i32),
                    _ => None,
                })
                .collect();
            Arc::new(Int32Array::from(v))
        }
        DataType::Int64 => {
            let v: Vec<Option<i64>> = col
                .iter()
                .map(|x| match x {
                    Some(DynVal::I64(n)) => Some(*n),
                    Some(DynVal::I32(n)) => Some(i64::from(*n)),
                    _ => None,
                })
                .collect();
            Arc::new(Int64Array::from(v))
        }
        DataType::Float32 => {
            let v: Vec<Option<f32>> = col
                .iter()
                .map(|x| match x {
                    Some(DynVal::F32(n)) => Some(*n),
                    Some(DynVal::F64(n)) => Some(*n as f32),
                    _ => None,
                })
                .collect();
            Arc::new(Float32Array::from(v))
        }
        DataType::Float64 => {
            let v: Vec<Option<f64>> = col
                .iter()
                .map(|x| match x {
                    Some(DynVal::F64(n)) => Some(*n),
                    Some(DynVal::F32(n)) => Some(f64::from(*n)),
                    _ => None,
                })
                .collect();
            Arc::new(Float64Array::from(v))
        }
        DataType::Utf8 => {
            let v: Vec<Option<&str>> = col
                .iter()
                .map(|x| match x {
                    Some(DynVal::Str(s)) => Some(s.as_str()),
                    _ => None,
                })
                .collect();
            Arc::new(StringArray::from(v))
        }
        DataType::LargeUtf8 => {
            let v: Vec<Option<&str>> = col
                .iter()
                .map(|x| match x {
                    Some(DynVal::Str(s)) => Some(s.as_str()),
                    _ => None,
                })
                .collect();
            Arc::new(LargeStringArray::from(v))
        }
        DataType::Boolean => {
            let v: Vec<Option<bool>> = col
                .iter()
                .map(|x| match x {
                    Some(DynVal::Bool(b)) => Some(*b),
                    _ => None,
                })
                .collect();
            Arc::new(BooleanArray::from(v))
        }
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let v: Vec<Option<i64>> = col
                .iter()
                .map(|x| match x {
                    Some(DynVal::TimestampUs(n)) => Some(*n),
                    Some(DynVal::I64(n)) => Some(*n),
                    _ => None,
                })
                .collect();
            let arr = TimestampMicrosecondArray::from(v);
            // Preserve the original timezone so the schema round-trips correctly.
            let arr = if let Some(tz_str) = tz {
                arr.with_timezone(tz_str.as_ref())
            } else {
                arr
            };
            Arc::new(arr)
        }
        DataType::Date32 => {
            let v: Vec<Option<i32>> = col
                .iter()
                .map(|x| match x {
                    Some(DynVal::Date32(n)) => Some(*n),
                    Some(DynVal::I64(n)) => Some(*n as i32),
                    Some(DynVal::I32(n)) => Some(*n),
                    _ => None,
                })
                .collect();
            Arc::new(Date32Array::from(v))
        }
        // Fallback: return nulls for unsupported types
        _ => {
            let v: Vec<Option<i64>> = vec![None; col.len()];
            Arc::new(Int64Array::from(v))
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// UsersTable – seeded in-memory table supporting full DML
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone)]
struct UserRow {
    rowid: i64,
    id: i64,
    name: String,
}

struct UsersTableData {
    rows: Vec<UserRow>,
    next_rowid: i64,
}

struct UsersTable {
    /// Full schema: rowid (is_rowid) + id + name
    schema: Arc<ArrowSchema>,
    data: Arc<Mutex<UsersTableData>>,
}

impl UsersTable {
    fn new() -> Self {
        let schema = Arc::new(ArrowSchema::new(vec![
            rowid_field(),
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let data = UsersTableData {
            rows: vec![
                UserRow { rowid: 1, id: 1, name: "Alice".into() },
                UserRow { rowid: 2, id: 2, name: "Bob".into() },
                UserRow { rowid: 3, id: 3, name: "Charlie".into() },
            ],
            next_rowid: 4,
        };
        UsersTable {
            schema,
            data: Arc::new(Mutex::new(data)),
        }
    }

    fn build_batch(&self, opts: &ScanOptions) -> std::result::Result<RecordBatch, AirportError> {
        let data = self.data.lock().unwrap();

        let rowids: Vec<i64> = data.rows.iter().map(|r| r.rowid).collect();
        let ids: Vec<i64> = data.rows.iter().map(|r| r.id).collect();
        let names: Vec<&str> = data.rows.iter().map(|r| r.name.as_str()).collect();

        let limit = if opts.limit > 0 {
            opts.limit as usize
        } else {
            rowids.len()
        };
        let end = limit.min(rowids.len());

        let rowid_arr: ArrayRef = Arc::new(Int64Array::from(rowids[..end].to_vec()));
        let id_arr: ArrayRef = Arc::new(Int64Array::from(ids[..end].to_vec()));
        let name_arr: ArrayRef = Arc::new(StringArray::from(names[..end].to_vec()));

        if opts.columns.is_empty() {
            return RecordBatch::try_new(
                self.schema.clone(),
                vec![rowid_arr, id_arr, name_arr],
            )
            .map_err(AirportError::Arrow);
        }

        // Projection
        let mut fields = Vec::new();
        let mut arrays: Vec<ArrayRef> = Vec::new();
        for col_name in &opts.columns {
            match col_name.as_str() {
                "rowid" => {
                    fields.push(self.schema.field(0).clone());
                    arrays.push(rowid_arr.clone());
                }
                "id" => {
                    fields.push(self.schema.field(1).clone());
                    arrays.push(id_arr.clone());
                }
                "name" => {
                    fields.push(self.schema.field(2).clone());
                    arrays.push(name_arr.clone());
                }
                _ => {}
            }
        }
        if fields.is_empty() {
            return RecordBatch::try_new(
                self.schema.clone(),
                vec![rowid_arr, id_arr, name_arr],
            )
            .map_err(AirportError::Arrow);
        }
        RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), arrays)
            .map_err(AirportError::Arrow)
    }
}

#[async_trait::async_trait]
impl Table for UsersTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn name(&self) -> &str {
        "users"
    }
    fn comment(&self) -> &str {
        "Seeded users table"
    }
    fn arrow_schema(&self, columns: &[&str]) -> Arc<ArrowSchema> {
        if columns.is_empty() {
            self.schema.clone()
        } else {
            project_schema(&self.schema, columns)
        }
    }
    async fn scan(&self, _ctx: &RequestContext, opts: &ScanOptions) -> Result<SendableRecordBatchStream> {
        // Log filter if present
        if let Some(ref filter_bytes) = opts.filter {
            if let Some(fp) = airport::filter::parse(filter_bytes) {
                let col_names = fp.column_bindings.join(", ");
                tracing::debug!(
                    "[UsersTable] filter pushdown: {} filter(s), columns: [{}]",
                    fp.filters.len(),
                    col_names
                );
            }
        }
        let batch = self.build_batch(opts)?;
        Ok(Box::pin(futures::stream::once(async move { Ok(batch) })))
    }
    fn as_insertable(&self) -> Option<&dyn InsertableTable> {
        Some(self)
    }
    fn as_updatable_batch(&self) -> Option<&dyn UpdatableBatchTable> {
        Some(self)
    }
    fn as_deletable_batch(&self) -> Option<&dyn DeletableBatchTable> {
        Some(self)
    }
    fn as_dynamic_table(&self) -> Option<&dyn DynamicTable> {
        Some(self)
    }
}

#[async_trait::async_trait]
impl InsertableTable for UsersTable {
    async fn insert(
        &self,
        _ctx: &RequestContext,
        rows: SendableRecordBatchStream,
        _opts: &DMLOptions,
    ) -> Result<DMLResult> {
        let batches: Vec<RecordBatch> = rows.try_collect().await?;
        let mut data = self.data.lock().unwrap();
        let mut count = 0i64;
        for batch in &batches {
            let schema = batch.schema();
            let id_idx = schema.fields().iter().position(|f| f.name() == "id");
            let name_idx = schema.fields().iter().position(|f| f.name() == "name");
            for i in 0..batch.num_rows() {
                let id = id_idx
                    .and_then(|idx| extract_i64(batch.column(idx).as_ref(), i))
                    .unwrap_or(0);
                let name = name_idx
                    .and_then(|idx| extract_str(batch.column(idx).as_ref(), i))
                    .unwrap_or_default();
                let rowid = data.next_rowid;
                data.next_rowid += 1;
                data.rows.push(UserRow { rowid, id, name });
                count += 1;
            }
        }
        tracing::debug!("[UsersTable] inserted {} row(s), total={}", count, data.rows.len());
        Ok(DMLResult { affected_rows: count, returning_data: None })
    }
}

#[async_trait::async_trait]
impl UpdatableBatchTable for UsersTable {
    async fn update(
        &self,
        _ctx: &RequestContext,
        rows: RecordBatch,
        _opts: &DMLOptions,
    ) -> Result<DMLResult> {
        let rowid_col = find_row_id_column(rows.schema().as_ref())
            .ok_or_else(|| AirportError::Internal("rowid column not found in UPDATE batch".into()))?;
        let rowid_arr = rows
            .column(rowid_col)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| AirportError::Internal("rowid is not Int64".into()))?;

        let id_col = rows.schema().fields().iter().position(|f| f.name() == "id");
        let name_col = rows
            .schema()
            .fields()
            .iter()
            .position(|f| f.name() == "name");

        let mut data = self.data.lock().unwrap();
        let mut affected = 0i64;
        for i in 0..rows.num_rows() {
            let rid = rowid_arr.value(i);
            if let Some(row) = data.rows.iter_mut().find(|r| r.rowid == rid) {
                if let Some(idx) = id_col {
                    if let Some(v) = extract_i64(rows.column(idx).as_ref(), i) {
                        row.id = v;
                    }
                }
                if let Some(idx) = name_col {
                    if let Some(v) = extract_str(rows.column(idx).as_ref(), i) {
                        row.name = v;
                    }
                }
                affected += 1;
            }
        }
        tracing::debug!("[UsersTable] updated {} row(s)", affected);
        Ok(DMLResult { affected_rows: affected, returning_data: None })
    }
}

#[async_trait::async_trait]
impl DeletableBatchTable for UsersTable {
    async fn delete(
        &self,
        _ctx: &RequestContext,
        rows: RecordBatch,
        _opts: &DMLOptions,
    ) -> Result<DMLResult> {
        let rowid_col = find_row_id_column(rows.schema().as_ref())
            .ok_or_else(|| AirportError::Internal("rowid column not found in DELETE batch".into()))?;
        let rowid_arr = rows
            .column(rowid_col)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| AirportError::Internal("rowid is not Int64".into()))?;

        let to_delete: HashSet<i64> = (0..rows.num_rows()).map(|i| rowid_arr.value(i)).collect();

        let mut data = self.data.lock().unwrap();
        let before = data.rows.len();
        data.rows.retain(|r| !to_delete.contains(&r.rowid));
        let deleted = (before - data.rows.len()) as i64;
        tracing::debug!("[UsersTable] deleted {} row(s), remaining={}", deleted, data.rows.len());
        Ok(DMLResult { affected_rows: deleted, returning_data: None })
    }
}

#[async_trait::async_trait]
impl DynamicTable for UsersTable {
    /// Users table cannot be dropped via DDL.
    async fn drop(&self, _ctx: &RequestContext, _opts: &DropTableOptions) -> Result<()> {
        Err(AirportError::NotSupported(
            "users table is a permanent table and cannot be dropped".into(),
        ))
    }
    async fn add_column(&self, _ctx: &RequestContext, _field: Field, _opts: &AddColumnOptions) -> Result<()> {
        Err(AirportError::NotSupported("ALTER TABLE not supported on users table".into()))
    }
    async fn remove_column(&self, _ctx: &RequestContext, _column_name: &str, _opts: &RemoveColumnOptions) -> Result<()> {
        Err(AirportError::NotSupported("ALTER TABLE not supported on users table".into()))
    }
    async fn rename_column(&self, _ctx: &RequestContext, _old: &str, _new: &str, _opts: &RenameColumnOptions) -> Result<()> {
        Err(AirportError::NotSupported("ALTER TABLE not supported on users table".into()))
    }
    async fn rename_table(&self, _ctx: &RequestContext, _new_name: &str, _opts: &RenameTableOptions) -> Result<()> {
        Err(AirportError::NotSupported("RENAME TABLE not supported on users table".into()))
    }
    async fn change_column_type(&self, _ctx: &RequestContext, _col: &str, _ty: DataType, _opts: &ChangeColumnTypeOptions) -> Result<()> {
        Ok(())
    }
    async fn set_not_null(&self, _ctx: &RequestContext, _col: &str, _opts: &SetNotNullOptions) -> Result<()> {
        Ok(())
    }
    async fn drop_not_null(&self, _ctx: &RequestContext, _col: &str, _opts: &DropNotNullOptions) -> Result<()> {
        Ok(())
    }
    async fn set_default(&self, _ctx: &RequestContext, _col: &str, _default_value: Option<&str>, _opts: &SetDefaultOptions) -> Result<()> {
        Ok(())
    }
    async fn add_field(&self, _ctx: &RequestContext, _col: &str, _field: Field, _opts: &AddFieldOptions) -> Result<()> {
        Err(AirportError::NotSupported("ADD FIELD not supported".into()))
    }
    async fn rename_field(&self, _ctx: &RequestContext, _col: &str, _old: &str, _new: &str, _opts: &RenameFieldOptions) -> Result<()> {
        Ok(())
    }
    async fn remove_field(&self, _ctx: &RequestContext, _col: &str, _field: &str, _opts: &RemoveFieldOptions) -> Result<()> {
        Ok(())
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// DynTable – dynamically-created in-memory table
// ─────────────────────────────────────────────────────────────────────────────

struct DynTableData {
    /// Full schema: rowid (is_rowid) + user columns
    schema: Arc<ArrowSchema>,
    /// Parallel column vectors; cols[0] = rowids
    cols: Vec<Vec<Option<DynVal>>>,
    num_rows: usize,
    next_rowid: i64,
}

struct DynTable {
    name: String,
    comment: String,
    data: Arc<Mutex<DynTableData>>,
    /// Weak reference back to parent schema data (breaks Arc cycle).
    parent: Weak<RwLock<UnifiedSchemaData>>,
    ver: VersionCounter,
}

impl DynTable {
    fn new(
        name: impl Into<String>,
        comment: impl Into<String>,
        user_schema: Arc<ArrowSchema>,
        parent: Weak<RwLock<UnifiedSchemaData>>,
        ver: VersionCounter,
    ) -> Self {
        // Prepend rowid field
        let mut fields = vec![rowid_field()];
        fields.extend(clone_fields(&user_schema));
        let schema = Arc::new(ArrowSchema::new(fields));
        let num_cols = schema.fields().len();
        DynTable {
            name: name.into(),
            comment: comment.into(),
            data: Arc::new(Mutex::new(DynTableData {
                schema,
                cols: vec![vec![]; num_cols],
                num_rows: 0,
                next_rowid: 1,
            })),
            parent,
            ver,
        }
    }
}

#[async_trait::async_trait]
impl Table for DynTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn comment(&self) -> &str {
        &self.comment
    }
    fn arrow_schema(&self, columns: &[&str]) -> Arc<ArrowSchema> {
        let data = self.data.lock().unwrap();
        if columns.is_empty() {
            data.schema.clone()
        } else {
            project_schema(&data.schema, columns)
        }
    }
    async fn scan(&self, _ctx: &RequestContext, opts: &ScanOptions) -> Result<SendableRecordBatchStream> {
        let batch = {
            let data = self.data.lock().unwrap();
            let n = data.num_rows;

            let (schema, col_indices): (Arc<ArrowSchema>, Vec<usize>) = if opts.columns.is_empty() {
                let idx: Vec<usize> = (0..data.schema.fields().len()).collect();
                (data.schema.clone(), idx)
            } else {
                let mut fields = Vec::new();
                let mut idx = Vec::new();
                for col_name in &opts.columns {
                    if let Some(i) =
                        data.schema.fields().iter().position(|f| f.name() == col_name)
                    {
                        fields.push(data.schema.field(i).clone());
                        idx.push(i);
                    }
                }
                if fields.is_empty() {
                    let all: Vec<usize> = (0..data.schema.fields().len()).collect();
                    (data.schema.clone(), all)
                } else {
                    (Arc::new(ArrowSchema::new(fields)), idx)
                }
            };

            let mut arrays: Vec<ArrayRef> = Vec::new();
            for &col_idx in &col_indices {
                let col_data: &[Option<DynVal>] = if data.cols[col_idx].len() >= n {
                    &data.cols[col_idx][..n]
                } else {
                    &data.cols[col_idx]
                };
                let field = data.schema.field(col_idx);
                // rowid column (idx 0) is always Int64
                let arr: ArrayRef = if col_idx == 0 {
                    let v: Vec<Option<i64>> = col_data
                        .iter()
                        .map(|x| match x {
                            Some(DynVal::I64(n)) => Some(*n),
                            _ => None,
                        })
                        .collect();
                    Arc::new(Int64Array::from(v))
                } else {
                    build_column_array(field, col_data)
                };
                arrays.push(arr);
            }

            RecordBatch::try_new(schema, arrays).map_err(AirportError::Arrow)?
        };
        Ok(Box::pin(futures::stream::once(async move { Ok(batch) })))
    }
    fn as_insertable(&self) -> Option<&dyn InsertableTable> {
        Some(self)
    }
    fn as_updatable_batch(&self) -> Option<&dyn UpdatableBatchTable> {
        Some(self)
    }
    fn as_deletable_batch(&self) -> Option<&dyn DeletableBatchTable> {
        Some(self)
    }
    fn as_dynamic_table(&self) -> Option<&dyn DynamicTable> {
        Some(self)
    }
}

#[async_trait::async_trait]
impl InsertableTable for DynTable {
    async fn insert(
        &self,
        _ctx: &RequestContext,
        rows: SendableRecordBatchStream,
        _opts: &DMLOptions,
    ) -> Result<DMLResult> {
        let batches: Vec<RecordBatch> = rows.try_collect().await?;
        let mut data = self.data.lock().unwrap();
        let mut count = 0i64;
        for batch in &batches {
            let batch_schema = batch.schema();
            for row_i in 0..batch.num_rows() {
                // Save num_rows as a local to avoid simultaneous borrow of data
                let row_idx = data.num_rows;
                // Extend all columns to hold the new row (fill with None)
                for col in &mut data.cols {
                    if col.len() <= row_idx {
                        col.push(None);
                    }
                }
                // Set rowid
                let rowid = data.next_rowid;
                data.next_rowid += 1;
                data.cols[0][row_idx] = Some(DynVal::I64(rowid));
                // Set user columns by name
                for batch_col in 0..batch_schema.fields().len() {
                    let col_name = batch_schema.field(batch_col).name().to_string();
                    if col_name == "rowid" {
                        continue;
                    }
                    if let Some(schema_idx) = data
                        .schema
                        .fields()
                        .iter()
                        .position(|f| f.name().as_str() == col_name.as_str())
                    {
                        let val = extract_dynval(batch.column(batch_col).as_ref(), row_i);
                        data.cols[schema_idx][row_idx] = val;
                    }
                }
                data.num_rows += 1;
                count += 1;
            }
        }
        tracing::debug!("[DynTable:{}] inserted {} row(s)", self.name, count);
        Ok(DMLResult { affected_rows: count, returning_data: None })
    }
}

#[async_trait::async_trait]
impl UpdatableBatchTable for DynTable {
    async fn update(
        &self,
        _ctx: &RequestContext,
        rows: RecordBatch,
        _opts: &DMLOptions,
    ) -> Result<DMLResult> {
        let rowid_col = find_row_id_column(rows.schema().as_ref())
            .ok_or_else(|| AirportError::Internal("rowid column not found in UPDATE batch".into()))?;
        let rowid_arr = rows
            .column(rowid_col)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| AirportError::Internal("rowid is not Int64".into()))?;

        let mut data = self.data.lock().unwrap();

        // Build rowid → row_index map from cols[0]
        let mut rid_map: HashMap<i64, usize> = HashMap::new();
        for (i, v) in data.cols[0][..data.num_rows].iter().enumerate() {
            if let Some(DynVal::I64(rid)) = v {
                rid_map.insert(*rid, i);
            }
        }

        let mut affected = 0i64;
        for batch_row in 0..rows.num_rows() {
            let rid = rowid_arr.value(batch_row);
            if let Some(&row_idx) = rid_map.get(&rid) {
                for batch_col in 0..rows.num_columns() {
                    if batch_col == rowid_col {
                        continue;
                    }
                    let col_name = rows.schema().field(batch_col).name().to_string();
                    if let Some(schema_idx) = data
                        .schema
                        .fields()
                        .iter()
                        .position(|f| f.name().as_str() == col_name.as_str())
                    {
                        let val = extract_dynval(rows.column(batch_col).as_ref(), batch_row);
                        if schema_idx < data.cols.len() && row_idx < data.cols[schema_idx].len() {
                            data.cols[schema_idx][row_idx] = val;
                        }
                    }
                }
                affected += 1;
            }
        }
        tracing::debug!("[DynTable:{}] updated {} row(s)", self.name, affected);
        Ok(DMLResult { affected_rows: affected, returning_data: None })
    }
}

#[async_trait::async_trait]
impl DeletableBatchTable for DynTable {
    async fn delete(
        &self,
        _ctx: &RequestContext,
        rows: RecordBatch,
        _opts: &DMLOptions,
    ) -> Result<DMLResult> {
        let rowid_col = find_row_id_column(rows.schema().as_ref())
            .ok_or_else(|| AirportError::Internal("rowid column not found in DELETE batch".into()))?;
        let rowid_arr = rows
            .column(rowid_col)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| AirportError::Internal("rowid is not Int64".into()))?;

        let to_delete: HashSet<i64> = (0..rows.num_rows()).map(|i| rowid_arr.value(i)).collect();

        let mut data = self.data.lock().unwrap();

        // Determine which rows to keep
        let keep: Vec<bool> = data.cols[0][..data.num_rows]
            .iter()
            .map(|v| match v {
                Some(DynVal::I64(rid)) => !to_delete.contains(rid),
                _ => true,
            })
            .collect();

        let n = data.num_rows;
        for col in &mut data.cols {
            let new_col: Vec<_> = col[..n]
                .iter()
                .zip(&keep)
                .filter_map(|(v, &k)| if k { Some(v.clone()) } else { None })
                .collect();
            *col = new_col;
        }

        let deleted = keep.iter().filter(|&&k| !k).count() as i64;
        data.num_rows -= deleted as usize;
        tracing::debug!("[DynTable:{}] deleted {} row(s)", self.name, deleted);
        Ok(DMLResult { affected_rows: deleted, returning_data: None })
    }
}

#[async_trait::async_trait]
impl DynamicTable for DynTable {
    async fn drop(&self, _ctx: &RequestContext, _opts: &DropTableOptions) -> Result<()> {
        if let Some(parent) = self.parent.upgrade() {
            let mut schema_data = parent.write().unwrap();
            // Use pointer identity to find the current map key.
            // self.name may be stale if the table was renamed, so we search
            // for the entry whose Arc<dyn Table> pointer equals this DynTable.
            let key = schema_data
                .tables
                .iter()
                .find(|(_, tbl)| {
                    tbl.as_any()
                        .downcast_ref::<DynTable>()
                        .map_or(false, |dt| std::ptr::eq(dt as *const DynTable, self as *const DynTable))
                })
                .map(|(k, _)| k.clone());
            if let Some(k) = key {
                schema_data.tables.remove(&k);
                tracing::debug!("[DynTable:{}] dropped (map key={:?})", self.name, k);
            }
        }
        self.ver.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn add_column(&self, _ctx: &RequestContext, field: Field, opts: &AddColumnOptions) -> Result<()> {
        let mut data = self.data.lock().unwrap();
        let exists = data.schema.fields().iter().any(|f| f.name() == field.name());
        if exists {
            if opts.ignore_if_exists {
                return Ok(());
            }
            return Err(AirportError::AlreadyExists(field.name().to_string()));
        }
        let mut fields = clone_fields(&data.schema);
        fields.push(field.clone());
        let meta = data.schema.metadata().clone();
        data.schema = Arc::new(ArrowSchema::new_with_metadata(fields, meta));
        let cur_rows = data.num_rows;
        data.cols.push(vec![None; cur_rows]);
        tracing::debug!("[DynTable:{}] add_column {}", self.name, field.name());
        self.ver.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn remove_column(&self, _ctx: &RequestContext, column_name: &str, opts: &RemoveColumnOptions) -> Result<()> {
        let mut data = self.data.lock().unwrap();
        let pos = data.schema.fields().iter().position(|f| f.name() == column_name);
        match pos {
            None | Some(0) => {
                // Don't remove rowid (index 0)
                if opts.ignore_not_found { return Ok(()); }
                return Err(AirportError::NotFound(column_name.to_string()));
            }
            Some(idx) => {
                let meta = data.schema.metadata().clone();
                let fields: Vec<Field> = data
                    .schema
                    .fields()
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| *i != idx)
                    .map(|(_, f)| f.as_ref().clone())
                    .collect();
                data.schema = Arc::new(ArrowSchema::new_with_metadata(fields, meta));
                data.cols.remove(idx);
            }
        }
        self.ver.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn rename_column(&self, _ctx: &RequestContext, old_name: &str, new_name: &str, opts: &RenameColumnOptions) -> Result<()> {
        let mut data = self.data.lock().unwrap();
        let idx = data.schema.fields().iter().position(|f| f.name() == old_name);
        let Some(idx) = idx else {
            if opts.ignore_not_found { return Ok(()); }
            return Err(AirportError::NotFound(old_name.to_string()));
        };
        // Reject rename if new_name already exists.
        if data.schema.fields().iter().any(|f| f.name() == new_name) {
            return Err(AirportError::AlreadyExists(new_name.to_string()));
        }
        let meta = data.schema.metadata().clone();
        let fields: Vec<Field> = data
            .schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| {
                if i == idx {
                    Field::new(new_name, f.data_type().clone(), f.is_nullable())
                        .with_metadata(f.metadata().clone())
                } else {
                    f.as_ref().clone()
                }
            })
            .collect();
        data.schema = Arc::new(ArrowSchema::new_with_metadata(fields, meta));
        self.ver.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn rename_table(&self, _ctx: &RequestContext, new_name: &str, _opts: &RenameTableOptions) -> Result<()> {
        if let Some(parent) = self.parent.upgrade() {
            let mut schema_data = parent.write().unwrap();
            // Reject if new_name already exists.
            if schema_data.tables.contains_key(new_name) {
                return Err(AirportError::AlreadyExists(new_name.to_string()));
            }
            if let Some(tbl) = schema_data.tables.remove(&self.name) {
                schema_data.tables.insert(new_name.to_string(), tbl);
            }
        }
        self.ver.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn change_column_type(&self, _ctx: &RequestContext, column_name: &str, new_type: DataType, opts: &ChangeColumnTypeOptions) -> Result<()> {
        let mut data = self.data.lock().unwrap();
        let idx = data.schema.fields().iter().position(|f| f.name() == column_name);
        let Some(idx) = idx else {
            if opts.ignore_not_found { return Ok(()); }
            return Err(AirportError::NotFound(column_name.to_string()));
        };
        let meta = data.schema.metadata().clone();
        let fields: Vec<Field> = data
            .schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| {
                if i == idx {
                    Field::new(f.name(), new_type.clone(), f.is_nullable())
                } else {
                    f.as_ref().clone()
                }
            })
            .collect();
        data.schema = Arc::new(ArrowSchema::new_with_metadata(fields, meta));
        self.ver.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn set_not_null(&self, _ctx: &RequestContext, _col: &str, _opts: &SetNotNullOptions) -> Result<()> {
        Ok(())
    }
    async fn drop_not_null(&self, _ctx: &RequestContext, _col: &str, _opts: &DropNotNullOptions) -> Result<()> {
        Ok(())
    }
    async fn set_default(&self, _ctx: &RequestContext, _col: &str, _default_value: Option<&str>, _opts: &SetDefaultOptions) -> Result<()> {
        Ok(())
    }
    async fn add_field(&self, _ctx: &RequestContext, _col: &str, _field: Field, _opts: &AddFieldOptions) -> Result<()> {
        Err(AirportError::NotSupported("ADD FIELD (struct field) not supported".into()))
    }
    async fn rename_field(&self, _ctx: &RequestContext, _col: &str, _old: &str, _new: &str, _opts: &RenameFieldOptions) -> Result<()> {
        Ok(())
    }
    async fn remove_field(&self, _ctx: &RequestContext, _col: &str, _field: &str, _opts: &RemoveFieldOptions) -> Result<()> {
        Ok(())
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// UnifiedSchemaData – interior-mutable schema state (tables map)
// ─────────────────────────────────────────────────────────────────────────────

struct UnifiedSchemaData {
    tables: HashMap<String, Arc<dyn Table>>,
}

// ─────────────────────────────────────────────────────────────────────────────
// UnifiedSchema – dynamic schema (supports CREATE TABLE)
// ─────────────────────────────────────────────────────────────────────────────

struct UnifiedSchema {
    name: String,
    comment: String,
    data: Arc<RwLock<UnifiedSchemaData>>,
    ver: VersionCounter,
    scalar_funcs: Vec<Arc<dyn ScalarFunction>>,
    table_funcs: Vec<Arc<dyn TableFunction>>,
    table_refs: Vec<Arc<dyn TableRef>>,
}

impl UnifiedSchema {
    fn new_app(ver: VersionCounter) -> Self {
        let users = Arc::new(UsersTable::new()) as Arc<dyn Table>;
        let mut tables: HashMap<String, Arc<dyn Table>> = HashMap::new();
        tables.insert("users".to_string(), users);

        UnifiedSchema {
            name: "app".to_string(),
            comment: "Application schema".to_string(),
            data: Arc::new(RwLock::new(UnifiedSchemaData { tables })),
            ver: ver.clone(),
            scalar_funcs: vec![Arc::new(MultiplyFunc)],
            table_funcs: vec![Arc::new(GenerateSeriesFunc)],
            table_refs: vec![Arc::new(SeriesRef)],
        }
    }

    fn new_empty(name: impl Into<String>, comment: impl Into<String>, ver: VersionCounter) -> Self {
        UnifiedSchema {
            name: name.into(),
            comment: comment.into(),
            data: Arc::new(RwLock::new(UnifiedSchemaData {
                tables: HashMap::new(),
            })),
            ver,
            scalar_funcs: vec![],
            table_funcs: vec![],
            table_refs: vec![],
        }
    }
}

#[async_trait::async_trait]
impl Schema for UnifiedSchema {
    fn name(&self) -> &str {
        &self.name
    }
    fn comment(&self) -> &str {
        &self.comment
    }
    async fn tables(&self, _ctx: &RequestContext) -> Result<Vec<Arc<dyn Table>>> {
        let data = self.data.read().unwrap();
        Ok(data.tables.values().cloned().collect())
    }
    async fn table(&self, _ctx: &RequestContext, name: &str) -> Result<Option<Arc<dyn Table>>> {
        let data = self.data.read().unwrap();
        Ok(data.tables.get(name).cloned())
    }
    async fn scalar_functions(&self, _ctx: &RequestContext) -> Result<Vec<Arc<dyn ScalarFunction>>> {
        Ok(self.scalar_funcs.clone())
    }
    async fn table_functions(&self, _ctx: &RequestContext) -> Result<Vec<Arc<dyn TableFunction>>> {
        Ok(self.table_funcs.clone())
    }
    async fn table_functions_in_out(&self, _ctx: &RequestContext) -> Result<Vec<Arc<dyn airport::catalog::function::TableFunctionInOut>>> {
        Ok(vec![])
    }
    async fn table_refs(&self, _ctx: &RequestContext) -> Result<Vec<Arc<dyn TableRef>>> {
        Ok(self.table_refs.clone())
    }
    fn as_dynamic_schema(&self) -> Option<&dyn DynamicSchema> {
        Some(self)
    }
}

#[async_trait::async_trait]
impl DynamicSchema for UnifiedSchema {
    async fn create_table(
        &self,
        _ctx: &RequestContext,
        name: &str,
        schema: Arc<ArrowSchema>,
        opts: &CreateTableOptions,
    ) -> Result<Arc<dyn Table>> {
        let mut data = self.data.write().unwrap();
        if data.tables.contains_key(name) {
            match opts.on_conflict {
                OnConflict::Ignore => return Ok(data.tables[name].clone()),
                _ => return Err(AirportError::AlreadyExists(name.to_string())),
            }
        }
        let tbl = Arc::new(DynTable::new(
            name,
            opts.comment.clone().unwrap_or_default(),
            schema,
            Arc::downgrade(&self.data),
            self.ver.clone(),
        ));
        data.tables.insert(name.to_string(), tbl.clone() as Arc<dyn Table>);
        self.ver.fetch_add(1, Ordering::SeqCst);
        tracing::debug!("[UnifiedSchema:{}] created table {}", self.name, name);
        Ok(tbl as Arc<dyn Table>)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// UnifiedCatalog – dynamic catalog (supports CREATE/DROP SCHEMA)
// ─────────────────────────────────────────────────────────────────────────────

struct UnifiedCatalogData {
    schemas: HashMap<String, Arc<UnifiedSchema>>,
}

struct UnifiedCatalog {
    data: Arc<RwLock<UnifiedCatalogData>>,
    ver: VersionCounter,
}

impl UnifiedCatalog {
    fn new() -> Self {
        let ver = Arc::new(AtomicU64::new(1));
        let app = Arc::new(UnifiedSchema::new_app(ver.clone()));
        let mut schemas = HashMap::new();
        schemas.insert("app".to_string(), app);
        UnifiedCatalog {
            data: Arc::new(RwLock::new(UnifiedCatalogData { schemas })),
            ver,
        }
    }
}

#[async_trait::async_trait]
impl Catalog for UnifiedCatalog {
    fn name(&self) -> &str {
        "demo"
    }
    async fn schemas(&self, _ctx: &RequestContext) -> Result<Vec<Arc<dyn Schema>>> {
        let data = self.data.read().unwrap();
        Ok(data
            .schemas
            .values()
            .map(|s| s.clone() as Arc<dyn Schema>)
            .collect())
    }
    async fn schema(&self, _ctx: &RequestContext, name: &str) -> Result<Option<Arc<dyn Schema>>> {
        let data = self.data.read().unwrap();
        Ok(data
            .schemas
            .get(name)
            .map(|s| s.clone() as Arc<dyn Schema>))
    }
    fn as_dynamic(&self) -> Option<&dyn DynamicCatalog> {
        Some(self)
    }
    /// Dynamic catalog: tell DuckDB to re-check the version before every query.
    fn version_info(&self) -> CatalogVersion {
        CatalogVersion {
            version: self.ver.load(Ordering::SeqCst),
            is_fixed: false,
        }
    }
}

#[async_trait::async_trait]
impl DynamicCatalog for UnifiedCatalog {
    async fn create_schema(
        &self,
        _ctx: &RequestContext,
        name: &str,
        comment: &str,
        opts: &CreateSchemaOptions,
    ) -> Result<()> {
        let mut data = self.data.write().unwrap();
        if data.schemas.contains_key(name) {
            match opts.on_conflict {
                OnConflict::Ignore => return Ok(()),
                _ => return Err(AirportError::AlreadyExists(name.to_string())),
            }
        }
        let schema = Arc::new(UnifiedSchema::new_empty(name, comment, self.ver.clone()));
        data.schemas.insert(name.to_string(), schema);
        self.ver.fetch_add(1, Ordering::SeqCst);
        tracing::debug!("[UnifiedCatalog] created schema {}", name);
        Ok(())
    }

    async fn drop_schema(
        &self,
        _ctx: &RequestContext,
        name: &str,
        opts: &DropSchemaOptions,
    ) -> Result<()> {
        let mut data = self.data.write().unwrap();
        if !data.schemas.contains_key(name) {
            if opts.ignore_not_found {
                return Ok(());
            }
            return Err(AirportError::NotFound(name.to_string()));
        }
        data.schemas.remove(name);
        self.ver.fetch_add(1, Ordering::SeqCst);
        tracing::debug!("[UnifiedCatalog] dropped schema {}", name);
        Ok(())
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Scalar function: MULTIPLY(x INT64, factor INT64) -> INT64
// ─────────────────────────────────────────────────────────────────────────────

struct MultiplyFunc;

#[async_trait::async_trait]
impl ScalarFunction for MultiplyFunc {
    fn name(&self) -> &str {
        "MULTIPLY"
    }
    fn comment(&self) -> &str {
        "Multiplies two INT64 values"
    }
    fn signature(&self) -> &FunctionSignature {
        use std::sync::OnceLock;
        static SIG: OnceLock<FunctionSignature> = OnceLock::new();
        SIG.get_or_init(|| FunctionSignature {
            parameters: vec![DataType::Int64, DataType::Int64],
            return_type: Some(DataType::Int64),
            variadic: false,
        })
    }
    async fn execute(&self, _ctx: &RequestContext, batch: &RecordBatch) -> Result<ArrayRef> {
        if batch.num_columns() < 2 {
            return Err(AirportError::InvalidParameters(
                "MULTIPLY requires 2 arguments".into(),
            ));
        }
        let a = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| AirportError::InvalidParameters("arg 0 must be Int64".into()))?;
        let b = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| AirportError::InvalidParameters("arg 1 must be Int64".into()))?;
        let result: Int64Array = (0..a.len())
            .map(|i| {
                if a.is_null(i) || b.is_null(i) {
                    None
                } else {
                    Some(a.value(i) * b.value(i))
                }
            })
            .collect();
        Ok(Arc::new(result))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Table function: GENERATE_SERIES(start, stop, step?) -> table(value INT64)
// ─────────────────────────────────────────────────────────────────────────────

struct GenerateSeriesFunc;

fn series_schema() -> Arc<ArrowSchema> {
    use std::sync::OnceLock;
    static SCHEMA: OnceLock<Arc<ArrowSchema>> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            Arc::new(ArrowSchema::new(vec![Field::new(
                "value",
                DataType::Int64,
                false,
            )]))
        })
        .clone()
}

fn json_to_i64(v: &serde_json::Value) -> Option<i64> {
    match v {
        serde_json::Value::Number(n) => n.as_i64().or_else(|| n.as_f64().map(|f| f as i64)),
        _ => None,
    }
}

#[async_trait::async_trait]
impl TableFunction for GenerateSeriesFunc {
    fn name(&self) -> &str {
        "GENERATE_SERIES"
    }
    fn comment(&self) -> &str {
        "Generates an integer series from start to stop (inclusive) with optional step"
    }
    fn signature(&self) -> &FunctionSignature {
        use std::sync::OnceLock;
        static SIG: OnceLock<FunctionSignature> = OnceLock::new();
        SIG.get_or_init(|| FunctionSignature {
            parameters: vec![DataType::Int64, DataType::Int64, DataType::Int64],
            return_type: None,
            variadic: false,
        })
    }
    async fn schema_for_parameters(
        &self,
        _ctx: &RequestContext,
        _params: &[serde_json::Value],
    ) -> Result<Arc<ArrowSchema>> {
        Ok(series_schema())
    }
    async fn execute(
        &self,
        _ctx: &RequestContext,
        params: &[serde_json::Value],
        _opts: &ScanOptions,
    ) -> Result<SendableRecordBatchStream> {
        let start = params.first().and_then(json_to_i64).unwrap_or(0);
        let stop = params.get(1).and_then(json_to_i64).unwrap_or(0);
        let step = params
            .get(2)
            .and_then(json_to_i64)
            .filter(|&s| s != 0)
            .unwrap_or(1);

        let mut values: Vec<i64> = Vec::new();
        if step > 0 {
            let mut i = start;
            while i <= stop {
                values.push(i);
                i += step;
            }
        } else {
            let mut i = start;
            while i >= stop {
                values.push(i);
                i += step;
            }
        }

        let arr: ArrayRef = Arc::new(Int64Array::from(values));
        let schema = series_schema();
        let batch =
            RecordBatch::try_new(schema, vec![arr]).map_err(AirportError::Arrow)?;
        Ok(Box::pin(futures::stream::once(async move { Ok(batch) })))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// TableRef: series_ref – delegates to DuckDB's generate_series(1, 100)
// ─────────────────────────────────────────────────────────────────────────────

struct SeriesRef;

#[async_trait::async_trait]
impl TableRef for SeriesRef {
    fn name(&self) -> &str {
        "series_ref"
    }
    fn comment(&self) -> &str {
        "Integer series 1..100 via DuckDB generate_series"
    }
    fn arrow_schema(&self) -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(vec![Field::new(
            "generate_series",
            DataType::Int64,
            false,
        )]))
    }
    async fn function_calls(
        &self,
        _ctx: &RequestContext,
        _req: &FunctionCallRequest,
    ) -> Result<Vec<FunctionCall>> {
        Ok(vec![FunctionCall {
            function_name: "generate_series".to_string(),
            args: vec![
                FunctionCallArg {
                    name: String::new(),
                    value: FunctionCallValue::Int64(1),
                    data_type: Some(DataType::Int64),
                },
                FunctionCallArg {
                    name: String::new(),
                    value: FunctionCallValue::Int64(100),
                    data_type: Some(DataType::Int64),
                },
            ],
        }])
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// main
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    // Parse --port <N> from command-line args
    let args: Vec<String> = env::args().collect();
    let port: u16 = args
        .windows(2)
        .find(|w| w[0] == "--port")
        .and_then(|w| w[1].parse().ok())
        .unwrap_or(50052);

    let catalog = Arc::new(UnifiedCatalog::new());

    // Attach a snapshot-based TransactionManager so that SQL ROLLBACK can undo DML.
    let tx_manager = Arc::new(InMemoryTransactionManager::new(catalog.clone()));

    let service = new_server(
        ServerConfig::new(catalog as Arc<dyn Catalog>)
            .with_address(format!("localhost:{port}"))
            .with_tx_manager(tx_manager as Arc<dyn airport::catalog::transaction::TransactionManager>),
    );

    let addr = format!("0.0.0.0:{port}").parse()?;
    tracing::info!("Airport unified server listening on :{}", port);
    tracing::info!("Schema: app | Table: users (id BIGINT, name VARCHAR) | Rows: 3");
    tracing::info!("Connect: ATTACH 'grpc://localhost:{port}' AS flight (TYPE AIRPORT)");

    tonic::transport::Server::builder()
        .add_service(service)
        .serve(addr)
        .await?;

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Snapshot types for transaction rollback
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone)]
struct UsersSnapshot {
    rows: Vec<UserRow>,
    next_rowid: i64,
}

#[derive(Clone)]
struct DynTableSnapshot {
    name: String,
    comment: String,
    schema: Arc<ArrowSchema>,
    cols: Vec<Vec<Option<DynVal>>>,
    num_rows: usize,
    next_rowid: i64,
}

#[derive(Clone)]
struct SchemaSnapshot {
    /// Snapshot of the seeded users table (None if schema has no users table).
    users: Option<UsersSnapshot>,
    /// Snapshots of dynamically-created tables.
    dyn_tables: Vec<DynTableSnapshot>,
}

#[derive(Clone)]
struct CatalogSnapshot {
    schemas: HashMap<String, SchemaSnapshot>,
}

// ─────────────────────────────────────────────────────────────────────────────
// InMemoryTransactionManager — snapshot-based rollback
// ─────────────────────────────────────────────────────────────────────────────

struct TxEntry {
    status: TransactionState,
    snapshot: Option<CatalogSnapshot>,
}

pub struct InMemoryTransactionManager {
    txs: Mutex<HashMap<String, TxEntry>>,
    catalog: Arc<UnifiedCatalog>,
}

impl InMemoryTransactionManager {
    pub fn new(catalog: Arc<UnifiedCatalog>) -> Self {
        Self {
            txs: Mutex::new(HashMap::new()),
            catalog,
        }
    }

    fn take_snapshot(&self) -> CatalogSnapshot {
        let cat_data = self.catalog.data.read().unwrap();
        let mut schemas = HashMap::new();
        for (name, schema) in &cat_data.schemas {
            schemas.insert(name.clone(), snapshot_schema(schema));
        }
        CatalogSnapshot { schemas }
    }

    fn restore_snapshot(&self, snap: CatalogSnapshot) {
        let mut cat_data = self.catalog.data.write().unwrap();

        // Restore each schema that existed at snapshot time.
        for (schema_name, schema_snap) in &snap.schemas {
            if let Some(schema) = cat_data.schemas.get(schema_name) {
                restore_schema(schema, schema_snap);
            }
        }

        // Remove schemas created during the transaction.
        cat_data.schemas.retain(|name, _| snap.schemas.contains_key(name));

        // Bump version so DuckDB refreshes its catalog cache.
        self.catalog.ver.fetch_add(1, Ordering::SeqCst);
    }
}

#[async_trait::async_trait]
impl TransactionManager for InMemoryTransactionManager {
    async fn begin_transaction(&self, _ctx: &RequestContext) -> airport::catalog::Result<String> {
        let snap = self.take_snapshot();
        let tx_id = uuid::Uuid::new_v4().to_string();

        let mut txs = self.txs.lock().unwrap();
        txs.insert(tx_id.clone(), TxEntry {
            status: TransactionState::Active,
            snapshot: Some(snap),
        });

        tracing::debug!("[TxManager] BEGIN {}", &tx_id[..8]);
        Ok(tx_id)
    }

    async fn commit_transaction(&self, _ctx: &RequestContext, tx_id: &str) -> airport::catalog::Result<()> {
        let mut txs = self.txs.lock().unwrap();
        if let Some(entry) = txs.get_mut(tx_id) {
            if entry.status == TransactionState::Active {
                entry.status = TransactionState::Committed;
                entry.snapshot = None; // release memory
                tracing::debug!("[TxManager] COMMIT {}", &tx_id[..8]);
            }
        }
        Ok(())
    }

    async fn rollback_transaction(&self, _ctx: &RequestContext, tx_id: &str) -> airport::catalog::Result<()> {
        let snap = {
            let mut txs = self.txs.lock().unwrap();
            if let Some(entry) = txs.get_mut(tx_id) {
                if entry.status != TransactionState::Active {
                    return Ok(()); // idempotent
                }
                let snap = entry.snapshot.take();
                entry.status = TransactionState::RolledBack;
                snap
            } else {
                return Ok(());
            }
        };

        if let Some(snap) = snap {
            self.restore_snapshot(snap);
            tracing::debug!("[TxManager] ROLLBACK {} (snapshot restored)", &tx_id[..8]);
        }
        Ok(())
    }

    async fn get_transaction_status(
        &self,
        _ctx: &RequestContext,
        tx_id: &str,
    ) -> airport::catalog::Result<Option<TransactionState>> {
        let txs = self.txs.lock().unwrap();
        Ok(txs.get(tx_id).map(|e| e.status))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Snapshot / restore helpers
// ─────────────────────────────────────────────────────────────────────────────

fn snapshot_schema(schema: &Arc<UnifiedSchema>) -> SchemaSnapshot {
    let data = schema.data.read().unwrap();

    let users = data.tables.get("users").and_then(|tbl| {
        tbl.as_any().downcast_ref::<UsersTable>().map(snapshot_users_table)
    });

    let dyn_tables = data.tables.iter()
        .filter(|(name, _)| name.as_str() != "users")
        .filter_map(|(name, tbl)| {
            tbl.as_any().downcast_ref::<DynTable>().map(|dt| snapshot_dyn_table(name, dt))
        })
        .collect();

    SchemaSnapshot { users, dyn_tables }
}

fn snapshot_users_table(t: &UsersTable) -> UsersSnapshot {
    let data = t.data.lock().unwrap();
    UsersSnapshot {
        rows: data.rows.clone(),
        next_rowid: data.next_rowid,
    }
}

fn snapshot_dyn_table(name: &str, t: &DynTable) -> DynTableSnapshot {
    let data = t.data.lock().unwrap();
    // Deep-copy the columnar data.
    let cols: Vec<Vec<Option<DynVal>>> = data.cols.iter().map(|col| col.clone()).collect();
    DynTableSnapshot {
        name: name.to_string(),
        comment: t.comment.clone(),
        schema: data.schema.clone(),
        cols,
        num_rows: data.num_rows,
        next_rowid: data.next_rowid,
    }
}

fn restore_schema(schema: &Arc<UnifiedSchema>, snap: &SchemaSnapshot) {
    let mut data = schema.data.write().unwrap();

    // Restore users table data (in-place, keeping the same Arc).
    if let Some(users_snap) = &snap.users {
        if let Some(tbl) = data.tables.get("users") {
            if let Some(users_tbl) = tbl.as_any().downcast_ref::<UsersTable>() {
                let mut users_data = users_tbl.data.lock().unwrap();
                users_data.rows = users_snap.rows.clone();
                users_data.next_rowid = users_snap.next_rowid;
            }
        }
    }

    // Rebuild dynamic tables from snapshot.
    let snap_names: HashSet<&str> = snap.dyn_tables.iter().map(|ds| ds.name.as_str()).collect();

    // Remove tables created during the transaction.
    data.tables.retain(|name, _| name == "users" || snap_names.contains(name.as_str()));

    // Restore data for tables that existed at snapshot time.
    for ds in &snap.dyn_tables {
        if let Some(tbl) = data.tables.get(&ds.name) {
            if let Some(dyn_tbl) = tbl.as_any().downcast_ref::<DynTable>() {
                let mut td = dyn_tbl.data.lock().unwrap();
                td.cols = ds.cols.clone();
                td.num_rows = ds.num_rows;
                td.next_rowid = ds.next_rowid;
            }
        } else {
            // Table was dropped during transaction — recreate it.
            let new_tbl = Arc::new(DynTable::new(
                ds.name.clone(),
                ds.comment.clone(),
                ds.schema.clone(),
                Arc::downgrade(&schema.data),
                schema.ver.clone(),
            ));
            {
                let mut td = new_tbl.data.lock().unwrap();
                td.cols = ds.cols.clone();
                td.num_rows = ds.num_rows;
                td.next_rowid = ds.next_rowid;
            }
            data.tables.insert(ds.name.clone(), new_tbl as Arc<dyn airport::catalog::table::Table>);
        }
    }
}
