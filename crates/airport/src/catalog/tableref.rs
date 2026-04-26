use crate::catalog::Result;
use crate::flight::context::RequestContext;
use arrow_schema::Schema as ArrowSchema;
use std::sync::Arc;

/// FunctionCallArg represents a single argument to a DuckDB function call.
#[derive(Debug, Clone)]
pub struct FunctionCallArg {
    /// Empty for positional, set for named arguments.
    pub name: String,
    /// The argument value.
    pub value: FunctionCallValue,
    /// Optional type hint.
    pub data_type: Option<arrow_schema::DataType>,
}

/// FunctionCallValue represents the value of a function argument.
#[derive(Debug, Clone)]
pub enum FunctionCallValue {
    String(String),
    Bool(bool),
    Int64(i64),
    UInt64(u64),
    Float64(f64),
    Bytes(Vec<u8>),
    List(Vec<FunctionCallValue>),
    Map(std::collections::HashMap<String, FunctionCallValue>),
    Null,
}

/// FunctionCall encodes a DuckDB function invocation.
#[derive(Debug, Clone)]
pub struct FunctionCall {
    pub function_name: String,
    pub args: Vec<FunctionCallArg>,
}

/// FunctionCallRequest is the context passed to TableRef.function_calls().
#[derive(Debug, Clone, Default)]
pub struct FunctionCallRequest {
    /// JSON filter predicates from DuckDB optimizer.
    pub filters: Option<Vec<u8>>,
    /// Projection pushdown.
    pub columns: Vec<String>,
    /// MessagePack-decoded params.
    pub parameters: Vec<serde_json::Value>,
    /// Time-travel info.
    pub time_point: Option<super::types::TimePoint>,
}

/// TableRef delegates data reading to DuckDB function calls via data:// URIs.
#[async_trait::async_trait]
pub trait TableRef: Send + Sync {
    fn name(&self) -> &str;
    fn comment(&self) -> &str;
    fn arrow_schema(&self) -> Arc<ArrowSchema>;

    /// Generate DuckDB function calls to execute for data retrieval.
    async fn function_calls(
        &self,
        ctx: &RequestContext,
        req: &FunctionCallRequest,
    ) -> Result<Vec<FunctionCall>>;
}

/// DynamicSchemaTableRef extends TableRef for refs with parameter-dependent schemas.
#[async_trait::async_trait]
pub trait DynamicSchemaTableRef: TableRef {
    async fn schema_for_request(
        &self,
        ctx: &RequestContext,
        req: &FunctionCallRequest,
    ) -> Result<Arc<ArrowSchema>>;
}
