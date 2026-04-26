use std::sync::Arc;

use airport::catalog::{
    dynamic::DynamicCatalog, CatalogVersion, Result as CatalogResult, Schema,
};
use airport::flight::context::RequestContext;

use crate::backend::Backend;

use super::{to_airport, DuckDbSchema};

/// Catalog implementation backed by a DuckDB database.
///
/// The catalog name is the one clients use in `ATTACH '...' AS <name> (TYPE AIRPORT)`.
///
/// "main" is DuckDB's built-in default schema. The Airport extension's SQL planner
/// conflicts with it (see airport-rs smoke-test issue #3), so we hide it — users must
/// put their tables in a user-defined schema (typically created via the ingestor).
pub struct DuckDbCatalog {
    name: String,
    backend: Backend,
}

impl DuckDbCatalog {
    pub fn new(name: impl Into<String>, backend: Backend) -> Arc<Self> {
        Arc::new(Self {
            name: name.into(),
            backend,
        })
    }
}

#[async_trait::async_trait]
impl airport::catalog::Catalog for DuckDbCatalog {
    fn name(&self) -> &str {
        &self.name
    }

    async fn schemas(&self, _ctx: &RequestContext) -> CatalogResult<Vec<Arc<dyn Schema>>> {
        let names = self
            .backend
            .with_reader(|conn| {
                let mut stmt = conn.prepare(
                    "SELECT schema_name \
                       FROM duckdb_schemas() \
                      WHERE NOT internal \
                        AND schema_name <> 'main' \
                      ORDER BY schema_name",
                )?;
                let rows: Vec<String> = stmt
                    .query_map([], |r| r.get::<_, String>(0))?
                    .collect::<duckdb::Result<_>>()?;
                Ok(rows)
            })
            .await
            .map_err(to_airport)?;

        let backend = self.backend.clone();
        Ok(names
            .into_iter()
            .map(|n| DuckDbSchema::new(n, backend.clone()) as Arc<dyn Schema>)
            .collect())
    }

    async fn schema(
        &self,
        _ctx: &RequestContext,
        name: &str,
    ) -> CatalogResult<Option<Arc<dyn Schema>>> {
        if name == "main" {
            return Ok(None);
        }
        let name_owned = name.to_string();
        let exists = self
            .backend
            .with_reader(move |conn| {
                let count: i64 = conn.query_row(
                    "SELECT COUNT(*) FROM duckdb_schemas() \
                      WHERE NOT internal AND schema_name = ?",
                    [&name_owned],
                    |r| r.get(0),
                )?;
                Ok(count > 0)
            })
            .await
            .map_err(to_airport)?;

        if !exists {
            return Ok(None);
        }
        Ok(Some(
            DuckDbSchema::new(name.to_string(), self.backend.clone()) as Arc<dyn Schema>,
        ))
    }

    fn as_dynamic(&self) -> Option<&dyn DynamicCatalog> {
        // Phase 2 will return Some(self). For now, DDL is not exposed via Airport.
        None
    }

    fn version_info(&self) -> CatalogVersion {
        // is_fixed=false tells DuckDB Airport extension to poll catalog_version before
        // each query and refresh the cached schema when this counter bumps. Every
        // write RPC in `write_plane::actions` calls `Backend::bump_catalog_epoch`.
        CatalogVersion {
            version: self.backend.catalog_epoch(),
            is_fixed: false,
        }
    }
}
