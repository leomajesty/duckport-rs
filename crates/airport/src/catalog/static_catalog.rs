use crate::catalog::function::{ScalarFunction, TableFunction, TableFunctionInOut};
use crate::catalog::table::{SendableRecordBatchStream, Table};
use crate::catalog::tableref::TableRef;
use crate::catalog::types::ScanOptions;
use crate::catalog::{Catalog, Result, Schema};
use crate::flight::context::RequestContext;
use arrow_schema::Schema as ArrowSchema;
use std::collections::HashMap;
use std::sync::Arc;

/// A simple static catalog implementation for testing and examples.
/// Built via the builder pattern and immutable after construction.

/// StaticCatalog is an immutable catalog built from a list of schemas.
pub struct StaticCatalog {
    name: String,
    schemas: HashMap<String, Arc<StaticSchema>>,
}

impl StaticCatalog {
    pub fn new(name: impl Into<String>, schemas: Vec<StaticSchema>) -> Self {
        let mut schema_map = HashMap::new();
        for schema in schemas {
            let arc = Arc::new(schema);
            schema_map.insert(arc.name().to_string(), arc);
        }
        StaticCatalog {
            name: name.into(),
            schemas: schema_map,
        }
    }
}

impl crate::catalog::NamedCatalog for StaticCatalog {
    fn name(&self) -> &str {
        &self.name
    }
}

#[async_trait::async_trait]
impl Catalog for StaticCatalog {
    fn name(&self) -> &str {
        &self.name
    }

    async fn schemas(&self, _ctx: &RequestContext) -> Result<Vec<Arc<dyn Schema>>> {
        Ok(self
            .schemas
            .values()
            .map(|s| s.clone() as Arc<dyn Schema>)
            .collect())
    }

    async fn schema(&self, _ctx: &RequestContext, name: &str) -> Result<Option<Arc<dyn Schema>>> {
        Ok(self
            .schemas
            .get(name)
            .map(|s| s.clone() as Arc<dyn Schema>))
    }
}

/// StaticSchema holds tables and functions.
pub struct StaticSchema {
    name: String,
    comment: String,
    tables: HashMap<String, Arc<dyn Table>>,
    scalar_functions: Vec<Arc<dyn ScalarFunction>>,
    table_functions: Vec<Arc<dyn TableFunction>>,
    table_functions_in_out: Vec<Arc<dyn TableFunctionInOut>>,
    table_refs: Vec<Arc<dyn TableRef>>,
}

impl StaticSchema {
    pub fn new(
        name: impl Into<String>,
        comment: impl Into<String>,
        tables: Vec<Arc<dyn Table>>,
    ) -> Self {
        let mut table_map = HashMap::new();
        for t in tables {
            table_map.insert(t.name().to_string(), t);
        }
        StaticSchema {
            name: name.into(),
            comment: comment.into(),
            tables: table_map,
            scalar_functions: vec![],
            table_functions: vec![],
            table_functions_in_out: vec![],
            table_refs: vec![],
        }
    }

    pub fn with_scalar_functions(mut self, funcs: Vec<Arc<dyn ScalarFunction>>) -> Self {
        self.scalar_functions = funcs;
        self
    }

    pub fn with_table_functions(mut self, funcs: Vec<Arc<dyn TableFunction>>) -> Self {
        self.table_functions = funcs;
        self
    }

    pub fn with_table_functions_in_out(mut self, funcs: Vec<Arc<dyn TableFunctionInOut>>) -> Self {
        self.table_functions_in_out = funcs;
        self
    }

    pub fn with_table_refs(mut self, refs: Vec<Arc<dyn TableRef>>) -> Self {
        self.table_refs = refs;
        self
    }
}

#[async_trait::async_trait]
impl Schema for StaticSchema {
    fn name(&self) -> &str {
        &self.name
    }

    fn comment(&self) -> &str {
        &self.comment
    }

    async fn tables(&self, _ctx: &RequestContext) -> Result<Vec<Arc<dyn Table>>> {
        Ok(self.tables.values().cloned().collect())
    }

    async fn table(
        &self,
        _ctx: &RequestContext,
        name: &str,
    ) -> Result<Option<Arc<dyn Table>>> {
        Ok(self.tables.get(name).cloned())
    }

    async fn scalar_functions(
        &self,
        _ctx: &RequestContext,
    ) -> Result<Vec<Arc<dyn ScalarFunction>>> {
        Ok(self.scalar_functions.clone())
    }

    async fn table_functions(
        &self,
        _ctx: &RequestContext,
    ) -> Result<Vec<Arc<dyn TableFunction>>> {
        Ok(self.table_functions.clone())
    }

    async fn table_functions_in_out(
        &self,
        _ctx: &RequestContext,
    ) -> Result<Vec<Arc<dyn TableFunctionInOut>>> {
        Ok(self.table_functions_in_out.clone())
    }

    async fn table_refs(&self, _ctx: &RequestContext) -> Result<Vec<Arc<dyn TableRef>>> {
        Ok(self.table_refs.clone())
    }
}

/// StaticTable - a simple immutable table backed by a scan function.
pub struct StaticTable {
    name: String,
    comment: String,
    schema: Arc<ArrowSchema>,
    scan_fn: Box<dyn Fn(RequestContext, ScanOptions) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<SendableRecordBatchStream>> + Send>> + Send + Sync>,
}

impl StaticTable {
    /// Creates a new StaticTable with a scan function.
    pub fn new<F, Fut>(
        name: impl Into<String>,
        comment: impl Into<String>,
        schema: Arc<ArrowSchema>,
        scan_fn: F,
    ) -> Self
    where
        F: Fn(RequestContext, ScanOptions) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<SendableRecordBatchStream>> + Send + 'static,
    {
        StaticTable {
            name: name.into(),
            comment: comment.into(),
            schema,
            scan_fn: Box::new(move |ctx, opts| Box::pin(scan_fn(ctx, opts))),
        }
    }
}

#[async_trait::async_trait]
impl Table for StaticTable {
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
        if columns.is_empty() {
            return self.schema.clone();
        }
        crate::catalog::types::project_schema(&self.schema, columns)
    }

    async fn scan(
        &self,
        ctx: &RequestContext,
        opts: &ScanOptions,
    ) -> Result<SendableRecordBatchStream> {
        (self.scan_fn)(ctx.clone(), opts.clone()).await
    }
}

/// Builder for creating static catalogs easily.
pub struct CatalogBuilder {
    name: String,
    schemas: Vec<StaticSchema>,
}

impl CatalogBuilder {
    pub fn new(name: impl Into<String>) -> Self {
        CatalogBuilder {
            name: name.into(),
            schemas: vec![],
        }
    }

    pub fn add_schema(mut self, schema: StaticSchema) -> Self {
        self.schemas.push(schema);
        self
    }

    pub fn build(self) -> StaticCatalog {
        StaticCatalog::new(self.name, self.schemas)
    }
}
