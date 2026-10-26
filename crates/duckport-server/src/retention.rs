//! Rolling-window retention: keep the last `keep_days` days of each configured table.
//!
//! ## How it works
//!
//! Policies live in `data.retention_policy` (one row per business table in schema `data`).
//! The time column and the latest data time come from `data.watermark`, so plugins do not
//! register anything extra. Every run appends one summary row per table to
//! `data.retention_log`. Both tables are created by the server at startup.
//!
//! Per table and run:
//!
//! 1. Skip the table if it has a PRIMARY KEY / UNIQUE constraint or an index. DuckDB 1.5
//!    does not vacuum deleted rows of indexed tables, so deleting would not free space.
//! 2. `cutoff = date(least(duck_time, now)) - keep_days`.
//! 3. Delete `[day, day + batch_days)` one batch at a time. Each DELETE commits on its own
//!    and the scheduler pauses between batches so ingestor writes interleave on the single
//!    writer connection.
//! 4. Optionally archive each batch to parquet first (written to `.tmp`, row count verified,
//!    renamed). A batch is only deleted once its archive is verified.
//! 5. CHECKPOINT and record free blocks before/after to confirm space was reclaimed. A
//!    checkpoint that runs while another transaction is open does not vacuum, so a run that
//!    deleted at least a row group's worth of rows without freeing blocks is logged as
//!    `ok_pending_reclaim`; the next run's CHECKPOINT picks it up.
//!
//! ## Env vars
//!
//! | Variable                            | Default | Description                                  |
//! |-------------------------------------|---------|----------------------------------------------|
//! | `DUCKPORT_RETENTION_ENABLED`        | false   | Start the scheduler                          |
//! | `DUCKPORT_RETENTION_DRY_RUN`        | true    | Count rows only; never archive or delete     |
//! | `DUCKPORT_RETENTION_INTERVAL_SECS`  | 3600    | Pause between scheduled runs (min 60)        |
//! | `DUCKPORT_RETENTION_MIN_KEEP_DAYS`  | 7       | Policies keeping fewer days are refused      |
//! | `DUCKPORT_RETENTION_BATCH_PAUSE_MS` | 200     | Pause between two DELETE batches             |
//!
//! `duckport.retention_run` triggers a run on demand (see `write_plane::actions`).

use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use chrono::{Days, NaiveDate, NaiveDateTime, Utc};
use duckdb::params;
use serde::Serialize;
use tokio::sync::{oneshot, Mutex};
use tracing::{error, info, warn};

use crate::backend::Backend;
use crate::config::Config;

/// DDL for the retention system tables. Run from `init_system_schema` on every start,
/// whether or not the scheduler is enabled, so policies can be written beforehand.
pub const SYSTEM_SCHEMA_SQL: &str = r#"
    CREATE TABLE IF NOT EXISTS data.retention_policy (
        table_name  VARCHAR PRIMARY KEY,
        keep_days   INTEGER NOT NULL,
        batch_days  INTEGER NOT NULL DEFAULT 1,
        archive_dir VARCHAR,
        enabled     BOOLEAN NOT NULL DEFAULT true,
        updated_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS data.retention_log (
        run_id             VARCHAR,
        table_name         VARCHAR,
        cutoff             DATE,
        first_day          DATE,
        last_day           DATE,
        rows_archived      BIGINT,
        rows_deleted       BIGINT,
        dry_run            BOOLEAN,
        free_blocks_before BIGINT,
        free_blocks_after  BIGINT,
        status             VARCHAR,
        error              VARCHAR,
        started_at         TIMESTAMP,
        finished_at        TIMESTAMP
    );
"#;

/// First scheduled run happens this long after startup.
const STARTUP_DELAY: Duration = Duration::from_secs(60);
/// `retention_log` rows older than this are pruned at the end of every run.
const LOG_KEEP_DAYS: u64 = 90;
/// DuckDB's default row group size.
const ROW_GROUP_ROWS: u64 = 122_880;

/// Only one run at a time, scheduled or triggered via `duckport.retention_run`.
static RUN_LOCK: Mutex<()> = Mutex::const_new(());

#[derive(Debug, Clone)]
pub struct Settings {
    pub enabled: bool,
    pub dry_run: bool,
    pub interval: Duration,
    pub min_keep_days: u32,
    pub batch_pause: Duration,
}

impl Settings {
    pub fn from_config(cfg: &Config) -> Self {
        Self {
            enabled: cfg.retention_enabled,
            dry_run: cfg.retention_dry_run,
            interval: cfg.retention_interval,
            min_keep_days: cfg.retention_min_keep_days,
            batch_pause: cfg.retention_batch_pause,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct RunReport {
    pub run_id: String,
    pub dry_run: bool,
    pub tables: Vec<TableReport>,
}

#[derive(Debug, Serialize)]
pub struct TableReport {
    pub table: String,
    pub status: &'static str,
    pub cutoff: Option<NaiveDate>,
    pub first_day: Option<NaiveDate>,
    pub last_day: Option<NaiveDate>,
    pub rows_archived: u64,
    /// Rows deleted, or rows that would be deleted in a dry run.
    pub rows_deleted: u64,
    pub free_blocks_before: Option<i64>,
    pub free_blocks_after: Option<i64>,
    pub error: Option<String>,
}

impl TableReport {
    fn new(table: &str) -> Self {
        Self {
            table: table.to_string(),
            status: "ok_nothing_to_do",
            cutoff: None,
            first_day: None,
            last_day: None,
            rows_archived: 0,
            rows_deleted: 0,
            free_blocks_before: None,
            free_blocks_after: None,
            error: None,
        }
    }

    fn with_status(mut self, status: &'static str) -> Self {
        self.status = status;
        self
    }
}

struct Policy {
    table: String,
    keep_days: i32,
    batch_days: i32,
    archive_dir: Option<String>,
    time_column: Option<String>,
    duck_time: Option<String>,
}

/// Spawn the retention scheduler as a background tokio task.
///
/// Sending on (or dropping) the returned sender stops the wait loop; an in-flight run is
/// allowed to finish first.
pub fn spawn(backend: Backend, settings: Settings) -> oneshot::Sender<()> {
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
    tokio::spawn(async move {
        let mut delay = STARTUP_DELAY;
        loop {
            info!(
                next_in_secs = delay.as_secs(),
                dry_run = settings.dry_run,
                "retention: next run scheduled"
            );
            tokio::select! {
                _ = &mut shutdown_rx => {
                    info!("retention: shutdown requested");
                    break;
                }
                _ = tokio::time::sleep(delay) => {}
            }
            if let Err(e) = run(&backend, &settings, None, settings.dry_run).await {
                error!(err = ?e, "retention: run failed");
            }
            delay = settings.interval;
        }
    });
    shutdown_tx
}

/// Run retention once for every enabled policy, or only for `table`.
pub async fn run(
    backend: &Backend,
    settings: &Settings,
    table: Option<String>,
    dry_run: bool,
) -> Result<RunReport> {
    let _guard = RUN_LOCK
        .try_lock()
        .map_err(|_| anyhow!("a retention run is already in progress"))?;
    if let Some(t) = &table {
        validate_ident(t)?;
    }

    let run_id = Utc::now().format("%Y%m%dT%H%M%S%.3fZ").to_string();
    let policies = load_policies(backend, table.clone()).await?;
    if let Some(t) = &table {
        if policies.is_empty() {
            bail!("no enabled retention policy for table '{t}'");
        }
    }
    if policies.is_empty() {
        info!("retention: no enabled policies");
    }

    let mut tables = Vec::with_capacity(policies.len());
    for policy in &policies {
        let started_at = now_utc();
        let report = match process_table(backend, settings, policy, dry_run).await {
            Ok(r) => r,
            Err(e) => {
                let mut r = TableReport::new(&policy.table).with_status("error");
                r.error = Some(format!("{e:#}"));
                r
            }
        };
        match report.status {
            "error" => error!(table = %report.table, err = report.error.as_deref().unwrap_or(""), "retention: table failed"),
            s if s.starts_with("skipped") => warn!(table = %report.table, status = s, "retention: table skipped"),
            s => info!(
                table = %report.table,
                status = s,
                dry_run,
                cutoff = ?report.cutoff,
                rows_deleted = report.rows_deleted,
                rows_archived = report.rows_archived,
                "retention: table done"
            ),
        }
        if let Err(e) = write_log(backend, &run_id, &report, dry_run, started_at).await {
            warn!(err = ?e, table = %report.table, "retention: could not write retention_log");
        }
        tables.push(report);
    }

    if let Err(e) = prune_log(backend).await {
        warn!(err = ?e, "retention: could not prune retention_log");
    }
    Ok(RunReport {
        run_id,
        dry_run,
        tables,
    })
}

async fn load_policies(backend: &Backend, table: Option<String>) -> Result<Vec<Policy>> {
    backend
        .with_reader(move |conn| {
            let mut sql = String::from(
                "SELECT p.table_name, p.keep_days, p.batch_days, p.archive_dir, \
                 w.time_column, CAST(w.duck_time AS VARCHAR) \
                 FROM data.retention_policy p \
                 LEFT JOIN data.watermark w ON w.table_name = p.table_name \
                 WHERE p.enabled",
            );
            if table.is_some() {
                sql.push_str(" AND p.table_name = ?");
            }
            sql.push_str(" ORDER BY p.table_name");
            let mut stmt = conn.prepare(&sql)?;
            let map = |row: &duckdb::Row<'_>| {
                Ok(Policy {
                    table: row.get(0)?,
                    keep_days: row.get(1)?,
                    batch_days: row.get(2)?,
                    archive_dir: row.get(3)?,
                    time_column: row.get(4)?,
                    duck_time: row.get(5)?,
                })
            };
            let rows = match &table {
                Some(t) => stmt.query_map(params![t], map)?.collect::<Result<Vec<_>, _>>()?,
                None => stmt.query_map([], map)?.collect::<Result<Vec<_>, _>>()?,
            };
            Ok(rows)
        })
        .await
        .context("load retention policies")
}

async fn process_table(
    backend: &Backend,
    settings: &Settings,
    policy: &Policy,
    dry_run: bool,
) -> Result<TableReport> {
    let mut report = TableReport::new(&policy.table);
    validate_ident(&policy.table)?;

    if policy.keep_days < settings.min_keep_days as i32 {
        return Ok(report.with_status("skipped_keep_days_below_min"));
    }
    let Some(time_column) = policy.time_column.clone() else {
        return Ok(report.with_status("skipped_no_watermark"));
    };
    validate_ident(&time_column)?;
    let Some(duck_time) = policy.duck_time.as_deref() else {
        return Ok(report.with_status("skipped_no_duck_time"));
    };

    // Never let a duck_time in the future move the cutoff forward.
    let reference = parse_timestamp(duck_time)?.min(now_utc()).date();
    let cutoff = reference
        .checked_sub_days(Days::new(policy.keep_days as u64))
        .ok_or_else(|| anyhow!("cutoff out of range"))?;
    report.cutoff = Some(cutoff);

    let table = policy.table.clone();
    let (exists, keyed) = backend
        .with_reader(move |conn| {
            let row = conn.query_row(
                "SELECT \
                   (SELECT count(*) FROM duckdb_tables() WHERE schema_name = 'data' AND table_name = ?), \
                   (SELECT count(*) FROM duckdb_constraints() WHERE schema_name = 'data' AND table_name = ? \
                      AND constraint_type IN ('PRIMARY KEY', 'UNIQUE')) \
                 + (SELECT count(*) FROM duckdb_indexes() WHERE schema_name = 'data' AND table_name = ?)",
                params![table, table, table],
                |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)),
            )?;
            Ok(row)
        })
        .await?;
    if exists == 0 {
        return Ok(report.with_status("skipped_missing_table"));
    }
    if keyed > 0 {
        return Ok(report.with_status("skipped_has_index"));
    }

    let target = Target {
        table: policy.table.clone(),
        qualified: format!("\"data\".\"{}\"", policy.table),
        column: format!("\"{time_column}\""),
    };

    // Start at the oldest day that still holds rows before the cutoff. The filter lets
    // zonemaps skip every row group at or after the cutoff.
    let first_day: Option<NaiveDate> = {
        let t = target.clone();
        let cutoff_s = fmt_day(cutoff);
        backend
            .with_reader(move |conn| {
                let v: Option<String> = conn.query_row(
                    &format!(
                        "SELECT CAST(CAST(min({col}) AS DATE) AS VARCHAR) FROM {qt} WHERE {col} < ?::TIMESTAMP",
                        col = t.column,
                        qt = t.qualified
                    ),
                    params![cutoff_s],
                    |r| r.get(0),
                )?;
                v.map(|s| parse_day(&s)).transpose()
            })
            .await?
    };
    let Some(mut day) = first_day else {
        return Ok(report.with_status(if dry_run { "dry_run" } else { "ok_nothing_to_do" }));
    };

    let free_before = if dry_run { None } else { Some(free_blocks(backend).await?) };
    let batch_days = policy.batch_days.max(1) as u64;
    while day < cutoff {
        let end = day
            .checked_add_days(Days::new(batch_days))
            .ok_or_else(|| anyhow!("day out of range"))?
            .min(cutoff);
        let (archived, deleted) = if dry_run {
            let t = target.clone();
            let n = backend
                .with_reader(move |conn| count_range(conn, &t, day, end))
                .await?;
            (0, n)
        } else {
            let t = target.clone();
            let dir = policy.archive_dir.clone();
            backend
                .with_writer(move |conn| delete_batch(conn, &t, day, end, dir.as_deref()))
                .await?
        };
        if deleted > 0 {
            if report.first_day.is_none() {
                report.first_day = Some(day);
            }
            report.last_day = end.pred_opt();
            report.rows_archived += archived;
            report.rows_deleted += deleted;
            if !dry_run {
                tokio::time::sleep(settings.batch_pause).await;
            }
        }
        day = end;
    }

    if dry_run {
        return Ok(report.with_status("dry_run"));
    }
    if report.rows_deleted == 0 {
        return Ok(report.with_status("ok_nothing_to_do"));
    }

    backend.bump_catalog_epoch();
    backend
        .with_writer(|conn| {
            conn.execute_batch("CHECKPOINT")?;
            Ok(())
        })
        .await
        .context("CHECKPOINT after retention")?;
    let free_after = free_blocks(backend).await?;
    report.free_blocks_before = free_before;
    report.free_blocks_after = Some(free_after);
    // Space is reclaimed per row group, so a run that deleted less than one row group's
    // worth of rows is not expected to free a block.
    let reclaimed = free_before.is_some_and(|b| free_after > b);
    let expect_reclaim = report.rows_deleted >= ROW_GROUP_ROWS;
    Ok(report.with_status(if reclaimed || !expect_reclaim {
        "ok"
    } else {
        "ok_pending_reclaim"
    }))
}

#[derive(Clone)]
struct Target {
    table: String,
    qualified: String,
    column: String,
}

fn count_range(conn: &duckdb::Connection, t: &Target, day: NaiveDate, end: NaiveDate) -> Result<u64> {
    let n: i64 = conn.query_row(
        &format!(
            "SELECT count(*) FROM {qt} WHERE {col} >= ?::TIMESTAMP AND {col} < ?::TIMESTAMP",
            qt = t.qualified,
            col = t.column
        ),
        params![fmt_day(day), fmt_day(end)],
        |r| r.get(0),
    )?;
    Ok(n as u64)
}

/// Archive (optionally) and delete one batch. Runs on the writer connection.
fn delete_batch(
    conn: &mut duckdb::Connection,
    t: &Target,
    day: NaiveDate,
    end: NaiveDate,
    archive_dir: Option<&str>,
) -> Result<(u64, u64)> {
    let mut archived = 0;
    if let Some(dir) = archive_dir.filter(|d| !d.trim().is_empty()) {
        let expected = count_range(conn, t, day, end)?;
        if expected > 0 {
            archived = archive_batch(conn, t, day, end, dir, expected)?;
        }
    }
    let deleted = conn.execute(
        &format!(
            "DELETE FROM {qt} WHERE {col} >= ?::TIMESTAMP AND {col} < ?::TIMESTAMP",
            qt = t.qualified,
            col = t.column
        ),
        params![fmt_day(day), fmt_day(end)],
    )?;
    Ok((archived, deleted as u64))
}

/// Write `[day, end)` to `<dir>/<table>/<table>_<day>.parquet` via a `.tmp` file and verify
/// the row count before renaming. An existing archive with the expected row count (left
/// by an interrupted run) is accepted as is.
fn archive_batch(
    conn: &duckdb::Connection,
    t: &Target,
    day: NaiveDate,
    end: NaiveDate,
    dir: &str,
    expected: u64,
) -> Result<u64> {
    let dir_path = PathBuf::from(dir).join(&t.table);
    fs::create_dir_all(&dir_path)
        .with_context(|| format!("create archive dir {}", dir_path.display()))?;
    let final_path = dir_path.join(format!("{}_{}.parquet", t.table, fmt_day(day)));
    if final_path.exists() {
        let n = parquet_rows(conn, &final_path)?;
        if n == expected {
            return Ok(n);
        }
        bail!(
            "archive {} already exists with {n} rows, expected {expected}; not deleting",
            final_path.display()
        );
    }
    let tmp_path = final_path.with_extension("parquet.tmp");
    conn.execute_batch(&format!(
        "COPY (SELECT * FROM {qt} WHERE {col} >= TIMESTAMP '{lo}' AND {col} < TIMESTAMP '{hi}' ORDER BY {col}) \
         TO '{path}' (FORMAT parquet)",
        qt = t.qualified,
        col = t.column,
        lo = fmt_day(day),
        hi = fmt_day(end),
        path = sql_path(&tmp_path)?
    ))
    .with_context(|| format!("COPY to {}", tmp_path.display()))?;
    let n = parquet_rows(conn, &tmp_path)?;
    if n != expected {
        let _ = fs::remove_file(&tmp_path);
        bail!(
            "archive {} has {n} rows, expected {expected}; not deleting",
            tmp_path.display()
        );
    }
    fs::rename(&tmp_path, &final_path)
        .with_context(|| format!("rename {} -> {}", tmp_path.display(), final_path.display()))?;
    Ok(n)
}

fn parquet_rows(conn: &duckdb::Connection, path: &Path) -> Result<u64> {
    let n: i64 = conn.query_row(
        &format!("SELECT count(*) FROM read_parquet('{}')", sql_path(path)?),
        [],
        |r| r.get(0),
    )?;
    Ok(n as u64)
}

async fn free_blocks(backend: &Backend) -> Result<i64> {
    backend
        .with_reader(|conn| {
            Ok(conn.query_row(
                "SELECT free_blocks FROM pragma_database_size() WHERE database_name = current_database()",
                [],
                |r| r.get(0),
            )?)
        })
        .await
        .context("read pragma_database_size")
}

async fn write_log(
    backend: &Backend,
    run_id: &str,
    r: &TableReport,
    dry_run: bool,
    started_at: NaiveDateTime,
) -> Result<()> {
    let row = (
        run_id.to_string(),
        r.table.clone(),
        r.cutoff.map(fmt_day),
        r.first_day.map(fmt_day),
        r.last_day.map(fmt_day),
        r.rows_archived as i64,
        r.rows_deleted as i64,
        dry_run,
        r.free_blocks_before,
        r.free_blocks_after,
        r.status.to_string(),
        r.error.clone(),
        fmt_ts(started_at),
        fmt_ts(now_utc()),
    );
    backend
        .with_writer(move |conn| {
            conn.execute(
                "INSERT INTO data.retention_log VALUES \
                 (?, ?, ?::DATE, ?::DATE, ?::DATE, ?, ?, ?, ?, ?, ?, ?, ?::TIMESTAMP, ?::TIMESTAMP)",
                params![
                    row.0, row.1, row.2, row.3, row.4, row.5, row.6, row.7, row.8, row.9, row.10,
                    row.11, row.12, row.13
                ],
            )?;
            Ok(())
        })
        .await
}

async fn prune_log(backend: &Backend) -> Result<()> {
    let before = now_utc()
        .checked_sub_days(Days::new(LOG_KEEP_DAYS))
        .map(fmt_ts)
        .ok_or_else(|| anyhow!("log prune cutoff out of range"))?;
    backend
        .with_writer(move |conn| {
            conn.execute(
                "DELETE FROM data.retention_log WHERE started_at < ?::TIMESTAMP",
                params![before],
            )?;
            Ok(())
        })
        .await
}

/// Identifiers are interpolated into SQL, so only plain names are accepted.
fn validate_ident(name: &str) -> Result<()> {
    let ok = !name.is_empty()
        && name.len() <= 128
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
    if ok {
        Ok(())
    } else {
        bail!("invalid identifier '{name}': only letters, digits and '_' are allowed")
    }
}

fn sql_path(path: &Path) -> Result<String> {
    let s = path
        .to_str()
        .ok_or_else(|| anyhow!("archive path is not valid UTF-8: {}", path.display()))?;
    Ok(s.replace('\'', "''"))
}

fn now_utc() -> NaiveDateTime {
    Utc::now().naive_utc()
}

fn fmt_day(d: NaiveDate) -> String {
    d.format("%Y-%m-%d").to_string()
}

fn fmt_ts(t: NaiveDateTime) -> String {
    t.format("%Y-%m-%d %H:%M:%S%.6f").to_string()
}

fn parse_day(s: &str) -> Result<NaiveDate> {
    NaiveDate::parse_from_str(s, "%Y-%m-%d").with_context(|| format!("cannot parse date '{s}'"))
}

fn parse_timestamp(s: &str) -> Result<NaiveDateTime> {
    NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S"))
        .with_context(|| format!("cannot parse timestamp '{s}'"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifiers_are_validated() {
        assert!(validate_ident("usdt_perp_1m").is_ok());
        assert!(validate_ident("open_time").is_ok());
        assert!(validate_ident("").is_err());
        assert!(validate_ident("t; DROP TABLE x").is_err());
        assert!(validate_ident("a\"b").is_err());
    }

    #[test]
    fn timestamps_parse_with_and_without_fraction() {
        assert!(parse_timestamp("2026-10-26 12:34:00").is_ok());
        assert!(parse_timestamp("2026-10-26 12:34:00.123456").is_ok());
        assert!(parse_timestamp("not a time").is_err());
    }

    #[test]
    fn archive_paths_escape_quotes() {
        assert_eq!(sql_path(Path::new("/a/b'c")).unwrap(), "/a/b''c");
    }
}
