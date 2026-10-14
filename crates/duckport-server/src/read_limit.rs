//! Public DoGet byte and timeout ceiling.
//!
//! The ceiling is stored in a task-local so Airport table scans, which do not
//! see the tonic request, pick up the same limits as `duckport.query`. Local
//! and LAN calls leave the task-local unset.

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use arrow_array::RecordBatch;
use duckdb::Connection;
use tonic::Status;

use crate::access::DoGetLimits;

tokio::task_local! {
    static ACTIVE_LIMITS: DoGetLimits;
}

pub async fn with_limits<F>(limits: DoGetLimits, fut: F) -> F::Output
where
    F: std::future::Future,
{
    ACTIVE_LIMITS.scope(limits, fut).await
}

pub fn current_limits() -> Option<DoGetLimits> {
    ACTIVE_LIMITS.try_with(|limits| *limits).ok()
}

#[derive(Debug)]
pub enum ScanFail {
    Internal(String),
    Bytes(String),
    Timeout(String),
}

impl ScanFail {
    pub fn marked(&self) -> String {
        match self {
            ScanFail::Internal(msg) => msg.clone(),
            ScanFail::Bytes(msg) => format!("resource_exhausted: {msg}"),
            ScanFail::Timeout(msg) => format!("deadline_exceeded: {msg}"),
        }
    }
}

/// Recover a gRPC status after Flight encoding has flattened the original code
/// into a string. The marker is placed in the error text by [`ScanFail::marked`].
pub fn recover_status(err: &str) -> Status {
    if let Some(idx) = err.find("resource_exhausted:") {
        Status::resource_exhausted(err[idx..].to_string())
    } else if let Some(idx) = err.find("deadline_exceeded:") {
        Status::deadline_exceeded(err[idx..].to_string())
    } else {
        Status::internal(err.to_string())
    }
}

struct InterruptWatch {
    cancel: Option<mpsc::Sender<()>>,
    join: Option<JoinHandle<()>>,
}

impl InterruptWatch {
    fn start(conn: &Connection, timeout: Duration, timed_out: &Arc<AtomicBool>) -> Self {
        if timeout.is_zero() {
            return Self {
                cancel: None,
                join: None,
            };
        }
        let handle = conn.interrupt_handle();
        let flag = Arc::clone(timed_out);
        let (cancel, rx) = mpsc::channel();
        let join = std::thread::spawn(move || {
            if let Err(RecvTimeoutError::Timeout) = rx.recv_timeout(timeout) {
                flag.store(true, Ordering::Release);
                handle.interrupt();
            }
        });
        Self {
            cancel: Some(cancel),
            join: Some(join),
        }
    }
}

impl Drop for InterruptWatch {
    fn drop(&mut self) {
        if let Some(cancel) = self.cancel.take() {
            let _ = cancel.send(());
        }
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

/// Run read-only SQL on `conn` and hand each batch to `emit`.
///
/// `emit` returns false when the caller no longer wants rows (client gone).
/// `limits` applies only when the caller already decided this is a public
/// DoGet. The interrupt watcher is joined before this function returns so the
/// connection is idle when the caller drops it back into the pool.
pub fn execute_arrow_query(
    conn: &Connection,
    sql: &str,
    limits: Option<DoGetLimits>,
    mut emit: impl FnMut(Result<RecordBatch, ScanFail>) -> bool,
) {
    let mut stmt = match conn.prepare(sql) {
        Ok(stmt) => stmt,
        Err(err) => {
            let _ = emit(Err(ScanFail::Internal(format!("prepare query: {err}"))));
            return;
        }
    };

    let limits = limits.filter(|limits| limits.max_bytes > 0 || !limits.timeout.is_zero());
    let timed_out = Arc::new(AtomicBool::new(false));
    let timeout = limits.map(|limits| limits.timeout).unwrap_or_default();
    let max_bytes = limits.map(|limits| limits.max_bytes).unwrap_or(0);
    let _watch = InterruptWatch::start(conn, timeout, &timed_out);

    let mut arrow = match stmt.query_arrow([]) {
        Ok(arrow) => arrow,
        Err(err) => {
            let _ = emit(Err(fail_exec(&timed_out, err.to_string())));
            return;
        }
    };

    let mut sent = 0u64;
    loop {
        if timed_out.load(Ordering::Acquire) {
            let _ = emit(Err(timeout_fail(timeout)));
            break;
        }
        let next = catch_unwind(AssertUnwindSafe(|| arrow.next()));
        match next {
            Ok(Some(batch)) => {
                if max_bytes > 0 {
                    let nbytes = batch.get_array_memory_size() as u64;
                    if sent.saturating_add(nbytes) > max_bytes {
                        let _ = emit(Err(ScanFail::Bytes(format!(
                            "public DoGet exceeded {max_bytes} bytes; narrow the query"
                        ))));
                        break;
                    }
                    sent = sent.saturating_add(nbytes);
                }
                if !emit(Ok(batch)) {
                    break;
                }
            }
            Ok(None) => break,
            Err(_) => {
                let fail = if timed_out.load(Ordering::Acquire) {
                    timeout_fail(timeout)
                } else {
                    ScanFail::Internal("arrow fetch failed".to_string())
                };
                let _ = emit(Err(fail));
                break;
            }
        }
    }
}

fn fail_exec(timed_out: &AtomicBool, err: String) -> ScanFail {
    if timed_out.load(Ordering::Acquire) {
        ScanFail::Timeout(format!("public DoGet timed out: {err}"))
    } else {
        ScanFail::Internal(format!("query_arrow: {err}"))
    }
}

fn timeout_fail(timeout: Duration) -> ScanFail {
    ScanFail::Timeout(format!(
        "public DoGet exceeded {}s timeout",
        timeout.as_secs().max(1)
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn collect(sql: &str, limits: Option<DoGetLimits>) -> Vec<Result<RecordBatch, ScanFail>> {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        let mut out = Vec::new();
        execute_arrow_query(&conn, sql, limits, |item| {
            out.push(item);
            true
        });
        out
    }

    #[test]
    fn unlimited_query_returns_rows() {
        let rows = collect("SELECT * FROM range(3)", None);
        assert!(rows.iter().all(|item| item.is_ok()));
        let total: usize = rows
            .iter()
            .map(|item| item.as_ref().unwrap().num_rows())
            .sum();
        assert_eq!(total, 3);
    }

    #[test]
    fn byte_cap_stops_without_sending_the_batch() {
        let rows = collect(
            "SELECT * FROM range(1000)",
            Some(DoGetLimits {
                max_bytes: 64,
                timeout: Duration::ZERO,
            }),
        );
        assert!(rows.iter().all(|item| item.is_err()));
        let msg = rows[0].as_ref().unwrap_err().marked();
        assert!(msg.starts_with("resource_exhausted:"), "{msg}");
        assert_eq!(recover_status(&msg).code(), tonic::Code::ResourceExhausted);
    }

    #[test]
    fn timeout_interrupts_a_long_scan() {
        let started = std::time::Instant::now();
        let rows = collect(
            "SELECT count(*) FROM range(100000000000)",
            Some(DoGetLimits {
                max_bytes: 0,
                timeout: Duration::from_millis(200),
            }),
        );
        assert!(
            started.elapsed() < Duration::from_secs(15),
            "interrupt did not stop the scan"
        );
        assert!(!rows.is_empty());
        let msg = rows
            .iter()
            .find_map(|item| item.as_ref().err())
            .expect("expected a timeout error")
            .marked();
        assert!(msg.contains("deadline_exceeded:"), "{msg}");
        assert_eq!(recover_status(&msg).code(), tonic::Code::DeadlineExceeded);
    }
}
