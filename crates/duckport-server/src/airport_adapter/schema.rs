use std::sync::Arc;

use airport::catalog::function::{ScalarFunction, TableFunction, TableFunctionInOut};
use airport::catalog::table::Table;
use airport::catalog::Result as CatalogResult;
use airport::flight::context::RequestContext;

use crate::backend::Backend;

use super::{to_airport, DuckDbTable};

/// Schema implementation backed by a DuckDB schema.
pub struct DuckDbSchema {
    name: String,
    backend: Backend,
}

impl DuckDbSchema {
    pub fn new(name: String, backend: Backend) -> Arc<Self> {
        Arc::new(Self { name, backend })
    }
}

#[async_trait::async_trait]
impl airport::catalog::Schema for DuckDbSchema {
    fn name(&self) -> &str {
        &self.name
    }

    fn comment(&self) -> &str {
        ""
    }

    async fn tables(&self, _ctx: &RequestContext) -> CatalogResult<Vec<Arc<dyn Table>>> {
        let schema_name = self.name.clone();
        let table_names = self
            .backend
            .with_reader(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT table_name \
                       FROM duckdb_tables() \
                      WHERE schema_name = ? AND NOT internal \
                      ORDER BY table_name",
                )?;
                let rows: Vec<String> = stmt
                    .query_map([&schema_name], |r| r.get::<_, String>(0))?
                    .collect::<duckdb::Result<_>>()?;
                Ok(rows)
            })
            .await
            .map_err(to_airport)?;

        let mut out: Vec<Arc<dyn Table>> = Vec::with_capacity(table_names.len());
        for tn in table_names {
            let table =
                DuckDbTable::load(self.backend.clone(), self.name.clone(), tn.clone()).await?;
            out.push(table as Arc<dyn Table>);
        }
        Ok(out)
    }

    async fn table(
        &self,
        _ctx: &RequestContext,
        name: &str,
    ) -> CatalogResult<Option<Arc<dyn Table>>> {
        let schema_name = self.name.clone();
        let table_name = name.to_string();
        let exists = self
            .backend
            .with_reader(move |conn| {
                let count: i64 = conn.query_row(
                    "SELECT COUNT(*) FROM duckdb_tables() \
                      WHERE schema_name = ? AND table_name = ? AND NOT internal",
                    [&schema_name, &table_name],
                    |r| r.get(0),
                )?;
                Ok(count > 0)
            })
            .await
            .map_err(to_airport)?;

        if !exists {
            return Ok(None);
        }
        let table =
            DuckDbTable::load(self.backend.clone(), self.name.clone(), name.to_string()).await?;
        Ok(Some(table as Arc<dyn Table>))
    }

    async fn scalar_functions(
        &self,
        _ctx: &RequestContext,
    ) -> CatalogResult<Vec<Arc<dyn ScalarFunction>>> {
        Ok(vec![])
    }

    async fn table_functions(
        &self,
        _ctx: &RequestContext,
    ) -> CatalogResult<Vec<Arc<dyn TableFunction>>> {
        Ok(vec![])
    }

    async fn table_functions_in_out(
        &self,
        _ctx: &RequestContext,
    ) -> CatalogResult<Vec<Arc<dyn TableFunctionInOut>>> {
        Ok(vec![])
    }
}
