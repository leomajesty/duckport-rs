use std::any::Any;
use std::sync::Arc;

use airport::catalog::table::{SendableRecordBatchStream, Table};
use airport::catalog::types::{project_schema, ScanOptions};
use airport::catalog::Result as CatalogResult;
use airport::error::AirportError;
use airport::flight::context::RequestContext;
use arrow_array::RecordBatch;
use arrow_schema::Schema as ArrowSchema;
use futures::stream;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, warn};

use crate::backend::Backend;

use super::{quote_ident, to_airport};

/// Channel buffer between the DuckDB blocking task and the async Flight stream.
/// Small value keeps memory bounded; DuckDB blocks on `send` if the client is slow.
const SCAN_CHANNEL_BUFFER: usize = 4;

pub struct DuckDbTable {
    backend: Backend,
    schema_name: String,
    table_name: String,
    arrow_schema: Arc<ArrowSchema>,
}

impl DuckDbTable {
    /// Load a table descriptor (including its Arrow schema) from DuckDB.
    ///
    /// The Arrow schema is resolved eagerly via `SELECT * FROM schema.table LIMIT 0`
    /// because the `Table::arrow_schema()` trait method is synchronous.
    pub async fn load(
        backend: Backend,
        schema_name: String,
        table_name: String,
    ) -> CatalogResult<Arc<Self>> {
        let sql = format!(
            "SELECT * FROM {}.{} LIMIT 0",
            quote_ident(&schema_name),
            quote_ident(&table_name),
        );
        let schema = backend
            .with_reader(move |conn| {
                let mut stmt = conn.prepare(&sql)?;
                let arrow = stmt.query_arrow([])?;
                let schema = arrow.get_schema();
                Ok(schema)
            })
            .await
            .map_err(to_airport)?;

        Ok(Arc::new(Self {
            backend,
            schema_name,
            table_name,
            arrow_schema: schema,
        }))
    }
}

#[async_trait::async_trait]
impl Table for DuckDbTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.table_name
    }

    fn comment(&self) -> &str {
        ""
    }

    fn arrow_schema(&self, columns: &[&str]) -> Arc<ArrowSchema> {
        if columns.is_empty() {
            self.arrow_schema.clone()
        } else {
            project_schema(&self.arrow_schema, columns)
        }
    }

    async fn scan(
        &self,
        _ctx: &RequestContext,
        opts: &ScanOptions,
    ) -> CatalogResult<SendableRecordBatchStream> {
        let sql = build_scan_sql(&self.schema_name, &self.table_name, opts, &self.arrow_schema);

        if opts.filter.is_some() {
            // Phase 1: filter pushdown is deferred. Log it so we notice on real workloads.
            // DuckDB clients apply the filter locally on the returned batches, so this is
            // correct but potentially wasteful.
            warn!(
                schema = %self.schema_name,
                table = %self.table_name,
                "filter pushdown not implemented yet; returning unfiltered rows"
            );
        }

        debug!(%sql, "duckport scan");

        let (tx, rx) = mpsc::channel::<Result<RecordBatch, AirportError>>(SCAN_CHANNEL_BUFFER);
        let backend = self.backend.clone();

        // Own the DuckDB connection + statement + arrow iterator entirely on a
        // blocking thread. Push batches through `tx`; stop early if the receiver drops
        // (i.e. the client disconnected).
        tokio::task::spawn_blocking(move || {
            let conn = match backend.reader() {
                Ok(c) => c,
                Err(e) => {
                    let _ = tx.blocking_send(Err(AirportError::Internal(format!(
                        "acquire duckdb conn: {e:#}"
                    ))));
                    return;
                }
            };

            let mut stmt = match conn.prepare(&sql) {
                Ok(s) => s,
                Err(e) => {
                    let _ = tx.blocking_send(Err(AirportError::Internal(format!(
                        "prepare scan: {e}"
                    ))));
                    return;
                }
            };

            let arrow_iter = match stmt.query_arrow([]) {
                Ok(a) => a,
                Err(e) => {
                    let _ = tx.blocking_send(Err(AirportError::Internal(format!(
                        "query_arrow: {e}"
                    ))));
                    return;
                }
            };

            for batch in arrow_iter {
                if tx.blocking_send(Ok(batch)).is_err() {
                    // Receiver dropped — client went away; stop walking the iterator.
                    break;
                }
            }
        });

        let stream = ReceiverStream::new(rx);
        Ok(Box::pin(stream) as SendableRecordBatchStream)
    }
}

fn build_scan_sql(
    schema: &str,
    table: &str,
    opts: &ScanOptions,
    full_schema: &ArrowSchema,
) -> String {
    // Column projection: intersect requested cols with the real schema to guard against
    // stale/garbage column names sent by a client.
    let col_list = if opts.columns.is_empty() {
        "*".to_string()
    } else {
        let valid: Vec<String> = opts
            .columns
            .iter()
            .filter(|c| full_schema.index_of(c.as_str()).is_ok())
            .map(|c| quote_ident(c))
            .collect();
        if valid.is_empty() {
            "*".to_string()
        } else {
            valid.join(", ")
        }
    };

    let mut sql = format!(
        "SELECT {col_list} FROM {sch}.{tbl}",
        sch = quote_ident(schema),
        tbl = quote_ident(table),
    );
    if opts.limit > 0 {
        sql.push_str(&format!(" LIMIT {}", opts.limit));
    }
    sql
}

// Silence clippy for an unused import on certain feature combinations.
#[allow(dead_code)]
fn _assert_send() {
    fn is_send<T: Send>() {}
    is_send::<stream::Empty<Result<RecordBatch, AirportError>>>();
}
