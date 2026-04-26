pub mod dynamic;
pub mod function;
pub mod geometry;
pub mod static_catalog;
pub mod table;
pub mod tableref;
pub mod transaction;
pub mod types;

#[allow(unused_imports)]
pub use dynamic::*;
pub use function::*;
pub use table::*;
pub use tableref::*;
pub use transaction::*;
pub use types::*;

use crate::catalog::dynamic::{DynamicCatalog, DynamicSchema};
use crate::error::AirportError;
use crate::flight::context::RequestContext;
use std::sync::Arc;

/// The default schema name used in Airport catalogs.
pub const DEFAULT_SCHEMA_NAME: &str = "main";

pub type Result<T> = std::result::Result<T, AirportError>;

/// Catalog represents the top-level metadata container.
/// All methods MUST be goroutine-safe (Send + Sync).
#[async_trait::async_trait]
pub trait Catalog: Send + Sync {
    /// Returns the catalog name, e.g. "demo".
    /// Override this in concrete implementations.
    fn name(&self) -> &str {
        ""
    }

    /// Returns all schemas visible in this catalog.
    async fn schemas(&self, ctx: &RequestContext) -> Result<Vec<Arc<dyn Schema>>>;

    /// Returns a specific schema by name.
    /// Returns Ok(None) if schema doesn't exist.
    async fn schema(
        &self,
        ctx: &RequestContext,
        name: &str,
    ) -> Result<Option<Arc<dyn Schema>>>;

    /// Returns this catalog as a DynamicCatalog if it supports schema DDL operations.
    fn as_dynamic(&self) -> Option<&dyn DynamicCatalog> {
        None
    }

    /// Returns catalog version information for DuckDB cache invalidation.
    /// Override for dynamic catalogs that support DDL (CREATE/DROP TABLE/SCHEMA).
    /// When is_fixed = false, DuckDB re-checks the version before every query
    /// and refreshes the catalog if the version has changed.
    fn version_info(&self) -> CatalogVersion {
        CatalogVersion { version: 1, is_fixed: true }
    }
}

/// NamedCatalog extends Catalog with a name.
pub trait NamedCatalog: Catalog {
    /// Returns the catalog name (e.g., "default", "analytics").
    fn name(&self) -> &str;
}

/// VersionedCatalog extends Catalog with version tracking.
#[async_trait::async_trait]
pub trait VersionedCatalog: Catalog {
    async fn catalog_version(&self, ctx: &RequestContext) -> Result<CatalogVersion>;
}

/// Returns catalog version info for cache invalidation.
/// Override for dynamic catalogs that support DDL.
/// Default: version 1, is_fixed true (DuckDB caches the catalog schema forever).
/// Set is_fixed = false so DuckDB polls catalog_version and refreshes after DDL.
pub fn default_version_info() -> CatalogVersion {
    CatalogVersion { version: 1, is_fixed: true }
}

/// Schema represents a database schema containing tables and functions.
#[async_trait::async_trait]
pub trait Schema: Send + Sync {
    /// Returns the schema name.
    fn name(&self) -> &str;

    /// Returns optional schema documentation.
    fn comment(&self) -> &str;

    /// Returns all tables in this schema.
    async fn tables(&self, ctx: &RequestContext) -> Result<Vec<Arc<dyn table::Table>>>;

    /// Returns a specific table by name.
    /// Returns Ok(None) if table doesn't exist.
    async fn table(
        &self,
        ctx: &RequestContext,
        name: &str,
    ) -> Result<Option<Arc<dyn table::Table>>>;

    /// Returns all scalar functions in this schema.
    async fn scalar_functions(
        &self,
        ctx: &RequestContext,
    ) -> Result<Vec<Arc<dyn function::ScalarFunction>>>;

    /// Returns all table-valued functions in this schema.
    async fn table_functions(
        &self,
        ctx: &RequestContext,
    ) -> Result<Vec<Arc<dyn function::TableFunction>>>;

    /// Returns all table functions that accept row sets as input.
    async fn table_functions_in_out(
        &self,
        ctx: &RequestContext,
    ) -> Result<Vec<Arc<dyn function::TableFunctionInOut>>>;

    /// Optional: Returns table references if this schema supports them.
    async fn table_refs(
        &self,
        _ctx: &RequestContext,
    ) -> Result<Vec<Arc<dyn tableref::TableRef>>> {
        Ok(vec![])
    }

    /// Returns this schema as a DynamicSchema if it supports table DDL operations.
    fn as_dynamic_schema(&self) -> Option<&dyn DynamicSchema> {
        None
    }
}
