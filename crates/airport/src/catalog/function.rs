use crate::catalog::{Result, ScanOptions};
use crate::flight::context::RequestContext;
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::Schema as ArrowSchema;
use std::sync::Arc;

use super::table::SendableRecordBatchStream;
use super::types::FunctionSignature;

/// ScalarFunction - a user-defined function returning scalar values per row.
#[async_trait::async_trait]
pub trait ScalarFunction: Send + Sync {
    fn name(&self) -> &str;
    fn comment(&self) -> &str;
    fn signature(&self) -> &FunctionSignature;

    /// Execute the scalar function on a RecordBatch.
    /// Returns an array with the same number of rows as input.
    async fn execute(&self, ctx: &RequestContext, batch: &RecordBatch) -> Result<ArrayRef>;
}

/// TableFunction - a user-defined function returning a table.
#[async_trait::async_trait]
pub trait TableFunction: Send + Sync {
    fn name(&self) -> &str;
    fn comment(&self) -> &str;
    fn signature(&self) -> &FunctionSignature;

    /// Returns the output schema for the given parameters.
    async fn schema_for_parameters(
        &self,
        ctx: &RequestContext,
        params: &[serde_json::Value],
    ) -> Result<Arc<ArrowSchema>>;

    /// Execute the table function with the given parameters.
    async fn execute(
        &self,
        ctx: &RequestContext,
        params: &[serde_json::Value],
        opts: &ScanOptions,
    ) -> Result<SendableRecordBatchStream>;
}

/// TableFunctionInOut - a function that transforms input row sets.
#[async_trait::async_trait]
pub trait TableFunctionInOut: Send + Sync {
    fn name(&self) -> &str;
    fn comment(&self) -> &str;
    fn signature(&self) -> &FunctionSignature;

    /// Returns the output schema for the given parameters and input schema.
    async fn schema_for_parameters(
        &self,
        ctx: &RequestContext,
        params: &[serde_json::Value],
        input_schema: &ArrowSchema,
    ) -> Result<Arc<ArrowSchema>>;

    /// Execute the function, transforming input batches to output.
    async fn execute(
        &self,
        ctx: &RequestContext,
        params: &[serde_json::Value],
        input: SendableRecordBatchStream,
        opts: &ScanOptions,
    ) -> Result<SendableRecordBatchStream>;
}
