use crate::catalog::Result;
use crate::catalog::table::Table;
use crate::catalog::types::{
    CreateSchemaOptions, CreateTableOptions, DropSchemaOptions,
};
use crate::flight::context::RequestContext;
use arrow_schema::Schema as ArrowSchema;
use std::sync::Arc;

/// DynamicCatalog extends Catalog with schema management capabilities.
#[async_trait::async_trait]
pub trait DynamicCatalog: Send + Sync {
    async fn create_schema(
        &self,
        ctx: &RequestContext,
        name: &str,
        comment: &str,
        opts: &CreateSchemaOptions,
    ) -> Result<()>;

    async fn drop_schema(
        &self,
        ctx: &RequestContext,
        name: &str,
        opts: &DropSchemaOptions,
    ) -> Result<()>;
}

/// DynamicSchema extends Schema with table management capabilities.
#[async_trait::async_trait]
pub trait DynamicSchema: Send + Sync {
    async fn create_table(
        &self,
        ctx: &RequestContext,
        name: &str,
        schema: Arc<ArrowSchema>,
        opts: &CreateTableOptions,
    ) -> Result<Arc<dyn Table>>;
}
