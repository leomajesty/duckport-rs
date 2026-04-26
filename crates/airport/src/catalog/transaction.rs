use crate::catalog::Result;
use crate::flight::context::RequestContext;

/// TransactionState represents the current state of a transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    Active,
    Committed,
    RolledBack,
}

/// TransactionManager coordinates multi-operation transactions.
#[async_trait::async_trait]
pub trait TransactionManager: Send + Sync {
    /// Begin a new transaction. Returns the transaction ID.
    async fn begin_transaction(&self, ctx: &RequestContext) -> Result<String>;

    /// Commit an active transaction.
    async fn commit_transaction(&self, ctx: &RequestContext, tx_id: &str) -> Result<()>;

    /// Rollback an active transaction.
    async fn rollback_transaction(&self, ctx: &RequestContext, tx_id: &str) -> Result<()>;

    /// Get the current status of a transaction.
    /// Returns (state, exists) tuple.
    async fn get_transaction_status(
        &self,
        ctx: &RequestContext,
        tx_id: &str,
    ) -> Result<Option<TransactionState>>;
}

/// CatalogTransactionManager is a multi-catalog aware transaction manager.
/// Routes transactions to the correct underlying manager based on catalog name.
#[async_trait::async_trait]
pub trait CatalogTransactionManager: Send + Sync {
    async fn begin_transaction(
        &self,
        ctx: &RequestContext,
        catalog_name: &str,
    ) -> Result<String>;

    async fn commit_transaction(
        &self,
        ctx: &RequestContext,
        catalog_name: &str,
        tx_id: &str,
    ) -> Result<()>;

    async fn rollback_transaction(
        &self,
        ctx: &RequestContext,
        catalog_name: &str,
        tx_id: &str,
    ) -> Result<()>;

    async fn get_transaction_status(
        &self,
        ctx: &RequestContext,
        catalog_name: &str,
        tx_id: &str,
    ) -> Result<Option<TransactionState>>;
}

/// Adapter wrapping CatalogTransactionManager into TransactionManager for a specific catalog.
pub struct CatalogTransactionManagerAdapter<M: CatalogTransactionManager> {
    inner: std::sync::Arc<M>,
    catalog_name: String,
}

impl<M: CatalogTransactionManager> CatalogTransactionManagerAdapter<M> {
    pub fn new(inner: std::sync::Arc<M>, catalog_name: impl Into<String>) -> Self {
        Self {
            inner,
            catalog_name: catalog_name.into(),
        }
    }
}

#[async_trait::async_trait]
impl<M: CatalogTransactionManager> TransactionManager for CatalogTransactionManagerAdapter<M> {
    async fn begin_transaction(&self, ctx: &RequestContext) -> Result<String> {
        self.inner.begin_transaction(ctx, &self.catalog_name).await
    }

    async fn commit_transaction(&self, ctx: &RequestContext, tx_id: &str) -> Result<()> {
        self.inner
            .commit_transaction(ctx, &self.catalog_name, tx_id)
            .await
    }

    async fn rollback_transaction(&self, ctx: &RequestContext, tx_id: &str) -> Result<()> {
        self.inner
            .rollback_transaction(ctx, &self.catalog_name, tx_id)
            .await
    }

    async fn get_transaction_status(
        &self,
        ctx: &RequestContext,
        tx_id: &str,
    ) -> Result<Option<TransactionState>> {
        self.inner
            .get_transaction_status(ctx, &self.catalog_name, tx_id)
            .await
    }
}
