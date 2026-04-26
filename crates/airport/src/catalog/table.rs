use crate::catalog::{Result, SchemaRequest};
use crate::catalog::types::{
    AddColumnOptions, AddFieldOptions, ChangeColumnTypeOptions, ColumnStats, DMLOptions,
    DMLResult, DropNotNullOptions, DropTableOptions, RemoveColumnOptions, RemoveFieldOptions,
    RenameColumnOptions, RenameFieldOptions, RenameTableOptions, ScanOptions, SetDefaultOptions,
    SetNotNullOptions,
};
use crate::flight::context::RequestContext;
use arrow_array::RecordBatch;
use arrow_schema::{Field, Schema as ArrowSchema};
use std::sync::Arc;

/// SendableRecordBatchStream is a stream of RecordBatch items.
pub type SendableRecordBatchStream =
    std::pin::Pin<Box<dyn futures::Stream<Item = Result<RecordBatch>> + Send>>;

/// Table represents a queryable table or view with fixed schema.
#[async_trait::async_trait]
pub trait Table: Send + Sync {
    /// Returns a reference to `self` as `dyn Any`, enabling downcasting.
    fn as_any(&self) -> &dyn std::any::Any;

    /// Returns the table name.
    fn name(&self) -> &str;

    /// Returns optional table documentation.
    fn comment(&self) -> &str;

    /// Returns the logical schema describing table columns.
    /// If columns is empty, returns full schema.
    /// If columns is provided, returns projected schema.
    fn arrow_schema(&self, columns: &[&str]) -> Arc<ArrowSchema>;

    /// Executes a scan operation and returns a stream of RecordBatches.
    async fn scan(
        &self,
        ctx: &RequestContext,
        opts: &ScanOptions,
    ) -> Result<SendableRecordBatchStream>;

    // --- Optional capabilities (correspond to Go type assertions) ---

    /// Returns this table as InsertableTable if it supports INSERT.
    fn as_insertable(&self) -> Option<&dyn InsertableTable> {
        None
    }

    /// Returns this table as UpdatableTable if it supports UPDATE (legacy).
    fn as_updatable(&self) -> Option<&dyn UpdatableTable> {
        None
    }

    /// Returns this table as DeletableTable if it supports DELETE (legacy).
    fn as_deletable(&self) -> Option<&dyn DeletableTable> {
        None
    }

    /// Returns this table as UpdatableBatchTable (preferred over UpdatableTable).
    fn as_updatable_batch(&self) -> Option<&dyn UpdatableBatchTable> {
        None
    }

    /// Returns this table as DeletableBatchTable (preferred over DeletableTable).
    fn as_deletable_batch(&self) -> Option<&dyn DeletableBatchTable> {
        None
    }

    /// Returns this table as StatisticsTable if it provides column statistics.
    fn as_statistics(&self) -> Option<&dyn StatisticsTable> {
        None
    }

    /// Returns this table as DynamicSchemaTable if schema depends on parameters/time.
    fn as_dynamic_schema(&self) -> Option<&dyn DynamicSchemaTable> {
        None
    }

    /// Returns this table as DynamicTable if it supports DDL column operations.
    fn as_dynamic_table(&self) -> Option<&dyn DynamicTable> {
        None
    }
}

/// DynamicSchemaTable extends Table for tables with parameter/time-dependent schemas.
#[async_trait::async_trait]
pub trait DynamicSchemaTable: Send + Sync {
    async fn schema_for_request(
        &self,
        ctx: &RequestContext,
        req: &SchemaRequest,
    ) -> Result<Arc<ArrowSchema>>;
}

/// InsertableTable extends Table with INSERT capability.
#[async_trait::async_trait]
pub trait InsertableTable: Send + Sync {
    async fn insert(
        &self,
        ctx: &RequestContext,
        rows: SendableRecordBatchStream,
        opts: &DMLOptions,
    ) -> Result<DMLResult>;
}

/// UpdatableTable extends Table with UPDATE capability (legacy - individual row IDs).
#[async_trait::async_trait]
pub trait UpdatableTable: Send + Sync {
    async fn update(
        &self,
        ctx: &RequestContext,
        row_ids: Vec<i64>,
        rows: SendableRecordBatchStream,
        opts: &DMLOptions,
    ) -> Result<DMLResult>;
}

/// DeletableTable extends Table with DELETE capability (legacy - individual row IDs).
#[async_trait::async_trait]
pub trait DeletableTable: Send + Sync {
    async fn delete(
        &self,
        ctx: &RequestContext,
        row_ids: Vec<i64>,
        opts: &DMLOptions,
    ) -> Result<DMLResult>;
}

/// UpdatableBatchTable extends Table with batch UPDATE capability (preferred).
#[async_trait::async_trait]
pub trait UpdatableBatchTable: Send + Sync {
    /// Update modifies existing rows using data from the RecordBatch.
    /// The RecordBatch contains both rowid column and new column values.
    async fn update(
        &self,
        ctx: &RequestContext,
        rows: RecordBatch,
        opts: &DMLOptions,
    ) -> Result<DMLResult>;
}

/// DeletableBatchTable extends Table with batch DELETE capability (preferred).
#[async_trait::async_trait]
pub trait DeletableBatchTable: Send + Sync {
    /// Delete removes rows identified by rowid values in the RecordBatch.
    async fn delete(
        &self,
        ctx: &RequestContext,
        rows: RecordBatch,
        opts: &DMLOptions,
    ) -> Result<DMLResult>;
}

/// StatisticsTable extends Table with column statistics capability.
#[async_trait::async_trait]
pub trait StatisticsTable: Send + Sync {
    async fn column_statistics(
        &self,
        ctx: &RequestContext,
        column_name: &str,
        column_type: &str,
    ) -> Result<ColumnStats>;
}

/// DynamicTable extends Table with DDL column operations capability.
#[async_trait::async_trait]
pub trait DynamicTable: Send + Sync {
    async fn drop(
        &self,
        ctx: &RequestContext,
        opts: &DropTableOptions,
    ) -> Result<()>;

    async fn add_column(
        &self,
        ctx: &RequestContext,
        field: Field,
        opts: &AddColumnOptions,
    ) -> Result<()>;

    async fn remove_column(
        &self,
        ctx: &RequestContext,
        column_name: &str,
        opts: &RemoveColumnOptions,
    ) -> Result<()>;

    async fn rename_column(
        &self,
        ctx: &RequestContext,
        old_name: &str,
        new_name: &str,
        opts: &RenameColumnOptions,
    ) -> Result<()>;

    async fn rename_table(
        &self,
        ctx: &RequestContext,
        new_name: &str,
        opts: &RenameTableOptions,
    ) -> Result<()>;

    async fn change_column_type(
        &self,
        ctx: &RequestContext,
        column_name: &str,
        new_type: arrow_schema::DataType,
        opts: &ChangeColumnTypeOptions,
    ) -> Result<()>;

    async fn set_not_null(
        &self,
        ctx: &RequestContext,
        column_name: &str,
        opts: &SetNotNullOptions,
    ) -> Result<()>;

    async fn drop_not_null(
        &self,
        ctx: &RequestContext,
        column_name: &str,
        opts: &DropNotNullOptions,
    ) -> Result<()>;

    async fn set_default(
        &self,
        ctx: &RequestContext,
        column_name: &str,
        default_value: Option<&str>,
        opts: &SetDefaultOptions,
    ) -> Result<()>;

    async fn add_field(
        &self,
        ctx: &RequestContext,
        column_name: &str,
        field: Field,
        opts: &AddFieldOptions,
    ) -> Result<()>;

    async fn rename_field(
        &self,
        ctx: &RequestContext,
        column_name: &str,
        old_field_name: &str,
        new_field_name: &str,
        opts: &RenameFieldOptions,
    ) -> Result<()>;

    async fn remove_field(
        &self,
        ctx: &RequestContext,
        column_name: &str,
        field_name: &str,
        opts: &RemoveFieldOptions,
    ) -> Result<()>;
}

/// Finds the rowid column in a RecordBatch schema.
/// Returns the column index if found.
pub fn find_row_id_column(schema: &ArrowSchema) -> Option<usize> {
    for (i, field) in schema.fields().iter().enumerate() {
        if field.name() == "rowid" {
            return Some(i);
        }
        if let Some(v) = field.metadata().get("is_rowid") {
            if !v.is_empty() {
                return Some(i);
            }
        }
    }
    None
}
