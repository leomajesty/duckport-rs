//! Periodic retention task: export oldest completed parquet period → delete from DuckDB.
//!
//! ## How it works
//!
//! Configuration lives in a DuckDB table (default `data.retention_tasks`).  Each row
//! describes one market/interval pair.  The scheduler re-reads this table at every run,
//! so new ingestors can register themselves by inserting a row — **no server restart
//! needed**.
//!
//! The table is managed by the Python ingestor via `DuckportClient.register_retention_task`.
//! Schema:
//!
//! ```sql
//! CREATE TABLE data.retention_tasks (
//!     market         VARCHAR   NOT NULL,
//!     interval       VARCHAR   NOT NULL,
//!     schema_name    VARCHAR   NOT NULL,   -- DuckDB schema, e.g. 'data'
//!     retention_days INTEGER   NOT NULL,
//!     parquet_dir    VARCHAR   NOT NULL,   -- root dir for parquet archives
//!     start_date     VARCHAR   NOT NULL,   -- data origin, 'YYYY-MM-DD'
//!     file_period    INTEGER   NOT NULL,   -- months per parquet file
//!     enabled        BOOLEAN   NOT NULL,
//!     updated_at     TIMESTAMP NOT NULL,
//!     PRIMARY KEY (market, interval)
//! );
//! ```
//!
//! ## Schedule
//!
//! Runs every 8 h at UTC boundaries (00:00 / 08:00 / 16:00) offset by +3 m 30 s,
//! matching the original Python schedule.
//!
//! ## Env vars
//!
//! | Variable                    | Default               | Description                        |
//! |-----------------------------|-----------------------|------------------------------------|
//! | DUCKPORT_RETENTION_ENABLED  | false (off)            | Set to `true` / `1` to enable      |
//! | DUCKPORT_RETENTION_TABLE    | data.retention_tasks  | Fully-qualified table name         |

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{anyhow, Result};
use chrono::{Datelike, NaiveDate, Timelike, Utc};
use tracing::{error, info, warn};

use crate::backend::Backend;

/// Spawn the retention scheduler as a background tokio task.
pub fn spawn(backend: Backend, table: String) {
    tokio::spawn(async move {
        loop {
            let delay = secs_until_next_run();
            let next = Utc::now() + chrono::Duration::seconds(delay as i64);
            info!(
                next = %next.format("%Y-%m-%d %H:%M:%S UTC"),
                %table,
                "retention: next run scheduled"
            );
            tokio::time::sleep(Duration::from_secs(delay)).await;

            if let Err(e) = run_once(&backend, &table).await {
                error!(err = ?e, "retention: run failed");
            }
        }
    });
}

/// Seconds from now until the next 8-hour UTC boundary + 3 m 30 s.
fn secs_until_next_run() -> u64 {
    let now = Utc::now();
    let h = now.hour();
    let next_boundary_h = ((h / 8) + 1) * 8;
    let secs_to_boundary = (next_boundary_h as i64 - h as i64) * 3600
        - now.minute() as i64 * 60
        - now.second() as i64;
    (secs_to_boundary + 210).max(1) as u64
}

// ── Per-tick logic ────────────────────────────────────────────────────────────

async fn run_once(backend: &Backend, table: &str) -> Result<()> {
    let tasks = load_tasks(backend, table).await?;
    if tasks.is_empty() {
        info!(%table, "retention: no enabled tasks");
        return Ok(());
    }
    info!(count = tasks.len(), "retention: starting periodic run");
    for task in &tasks {
        if let Err(e) = process_task(backend, task).await {
            error!(market = %task.market, err = ?e, "retention: task error");
        }
    }
    Ok(())
}

// ── Task row ─────────────────────────────────────────────────────────────────

struct TaskRow {
    market: String,
    interval: String,
    schema_name: String,
    retention_days: i32,
    parquet_dir: String,
    start_date_str: String,
    file_period: i32,
}

async fn load_tasks(backend: &Backend, table: &str) -> Result<Vec<TaskRow>> {
    let sql = format!(
        "SELECT market, interval, schema_name, retention_days, parquet_dir, \
         start_date, file_period \
         FROM {table} WHERE enabled = true"
    );
    backend
        .with_reader(move |conn| {
            let mut stmt = match conn.prepare(&sql) {
                Ok(s) => s,
                Err(_) => {
                    // Table likely doesn't exist yet — silently skip.
                    return Ok(vec![]);
                }
            };
            let mut rows = stmt.query([])?;
            let mut tasks = Vec::new();
            while let Some(row) = rows.next()? {
                tasks.push(TaskRow {
                    market: row.get(0)?,
                    interval: row.get(1)?,
                    schema_name: row.get(2)?,
                    retention_days: row.get(3)?,
                    parquet_dir: row.get(4)?,
                    start_date_str: row.get(5)?,
                    file_period: row.get(6)?,
                });
            }
            Ok(tasks)
        })
        .await
}

// ── Per-task logic ────────────────────────────────────────────────────────────

async fn process_task(backend: &Backend, task: &TaskRow) -> Result<()> {
    let start_date = NaiveDate::parse_from_str(&task.start_date_str, "%Y-%m-%d")
        .map_err(|e| anyhow!("invalid start_date '{}': {e}", task.start_date_str))?;

    // Read current duck_time from config_dict.
    let schema = task.schema_name.clone();
    let key = format!("{}_duck_time", task.market);
    let duck_sql = format!(
        "SELECT value FROM {schema}.config_dict WHERE key = '{key}'"
    );
    let duck_time_str: Option<String> = backend
        .with_reader(move |conn| {
            match conn.query_row(&duck_sql, [], |row| row.get::<_, String>(0)) {
                Ok(v) => Ok(Some(v)),
                Err(duckdb::Error::QueryReturnedNoRows) => Ok(None),
                Err(e) => Err(anyhow!("read duck_time: {e}")),
            }
        })
        .await?;

    let duck_time_str = match duck_time_str {
        Some(s) => s,
        None => {
            warn!(market = %task.market, "retention: duck_time not found, skipping");
            return Ok(());
        }
    };
    let duck_time = parse_naive_dt(&duck_time_str)
        .ok_or_else(|| anyhow!("cannot parse duck_time '{duck_time_str}'"))?;

    let cutoff = duck_time.date() - chrono::Duration::days(task.retention_days as i64);

    let pqt = match parquet_file_for(cutoff, &task.parquet_dir, &task.market, &task.interval, start_date, task.file_period) {
        Some(p) => p,
        None => {
            info!(market = %task.market, "retention: no completed parquet period yet, skipping");
            return Ok(());
        }
    };

    if pqt.path.exists() {
        info!(
            market = %task.market,
            file = %pqt.path.display(),
            "retention: parquet already exists, skipping"
        );
        return Ok(());
    }

    if let Some(parent) = pqt.path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let file_path = pqt.path.to_string_lossy().to_string();
    let target = format!("{}.{}_{}", task.schema_name, task.market, task.interval);
    let period_start = pqt.period_start.format("%Y-%m-%d").to_string();
    let period_end = pqt.period_end.format("%Y-%m-%d").to_string();

    info!(
        market = %task.market,
        %file_path, %period_start, %period_end,
        "retention: exporting parquet"
    );

    let copy_sql = format!(
        "COPY (SELECT * FROM {target} \
         WHERE open_time >= '{period_start}' AND open_time < '{period_end}' \
         ORDER BY open_time) TO '{file_path}' (FORMAT parquet)"
    );
    let delete_sql = format!(
        "DELETE FROM {target} \
         WHERE open_time < ('{period_end}'::timestamp - INTERVAL '1 day')"
    );

    backend
        .with_writer(move |conn| {
            conn.execute(&copy_sql, [])?;
            conn.execute(&delete_sql, [])?;
            Ok(())
        })
        .await?;

    info!(market = %task.market, %file_path, "retention: done");
    Ok(())
}

// ── Parquet period calculation ────────────────────────────────────────────────

struct ParquetFileInfo {
    path: PathBuf,
    period_start: NaiveDate,
    period_end: NaiveDate,
}

/// Compute the latest *completed* parquet period relative to `given_date`,
/// mirroring the Python `_calculate_theoretical_latest_file` logic.
fn parquet_file_for(
    given_date: NaiveDate,
    parquet_dir: &str,
    market: &str,
    interval: &str,
    start_date: NaiveDate,
    file_period: i32,
) -> Option<ParquetFileInfo> {
    if given_date < start_date {
        return None;
    }

    let months_since_start = (given_date.year() - start_date.year()) * 12
        + given_date.month() as i32
        - start_date.month() as i32;

    let mut complete_periods = months_since_start / file_period;

    // Go back one period to get the last *completed* period.
    complete_periods -= 1;
    // If exactly on a period boundary (day 1), go back one more.
    if months_since_start % file_period == 0 && given_date.day() == 1 {
        complete_periods -= 1;
    }

    if complete_periods < 0 {
        return None;
    }

    let total_months = complete_periods * file_period;
    let mut ps_year = start_date.year() + total_months / 12;
    let mut ps_month = start_date.month() as i32 + total_months % 12;
    if ps_month > 12 {
        ps_year += 1;
        ps_month -= 12;
    }
    let period_start = NaiveDate::from_ymd_opt(ps_year, ps_month as u32, 1)?;

    // period_end = period_start + file_period months (exclusive upper bound).
    let mut pe_year = ps_year;
    let mut pe_month = ps_month + file_period;
    while pe_month > 12 {
        pe_year += 1;
        pe_month -= 12;
    }
    let period_end = NaiveDate::from_ymd_opt(pe_year, pe_month as u32, 1)?;

    let period_str = period_start.format("%Y-%m").to_string();
    let filename = format!("{market}_{period_str}_{file_period}M.parquet");
    let path = PathBuf::from(parquet_dir)
        .join(format!("{market}_{interval}"))
        .join(filename);

    Some(ParquetFileInfo { path, period_start, period_end })
}

fn parse_naive_dt(s: &str) -> Option<chrono::NaiveDateTime> {
    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
        .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S"))
        .ok()
}
