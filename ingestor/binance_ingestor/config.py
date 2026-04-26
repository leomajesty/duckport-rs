"""
Binance Ingestor configuration.

Loaded from environment variables (via .env file or shell exports).
No argparse — config is purely env-driven so it can be imported safely
without side effects during tests or library usage.
"""

import os
import platform
import asyncio
from datetime import datetime, timezone

import pandas as pd
from dotenv import load_dotenv

# Load .env if present (no-op if missing)
_env_file = os.getenv("INGESTOR_ENV_FILE", "config.env")
load_dotenv(_env_file, override=False)


def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "on")

# ── duckport-rs server ────────────────────────────────────────────────
DUCKPORT_ADDR = os.getenv("DUCKPORT_ADDR", "localhost:50051")
DUCKPORT_SCHEMA = os.getenv("DUCKPORT_SCHEMA", "data")

# ── Kline settings ────────────────────────────────────────────────────
KLINE_INTERVAL = os.getenv("KLINE_INTERVAL", "5m")
KLINE_INTERVAL_MINUTES = int(KLINE_INTERVAL.replace('m', ''))
SUFFIX = f"_{KLINE_INTERVAL_MINUTES}m"

# ── Parquet archive directory (must be accessible to duckport-rs) ─────
PARQUET_DIR = os.getenv("PARQUET_DIR", "data/pqt")
if not os.path.isabs(PARQUET_DIR):
    PARQUET_DIR = os.path.join(os.getcwd(), PARQUET_DIR)

# ── Concurrency ───────────────────────────────────────────────────────
CONCURRENCY = int(os.getenv('CONCURRENCY', 2))
FETCH_CONCURRENCY = min(CONCURRENCY, 10)

# ── Data sources ──────────────────────────────────────────────────────
DATA_SOURCES_STR = os.getenv('DATA_SOURCES', 'usdt_perp,usdt_spot')
DATA_SOURCES = set(s.strip() for s in DATA_SOURCES_STR.split(','))

# ── Platform ──────────────────────────────────────────────────────────
if platform.system() == 'Windows':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# ── Retention (Plan A: off by default; no COPY+DELETE from hot DB) ──────
# Set RETENTION_ENABLED=true to register retention_tasks and use execute_retention.
RETENTION_ENABLED = _env_bool("RETENTION_ENABLED", False)
# Only used when RETENTION_ENABLED; kept for backward-compatible env files.
RETENTION_DAYS = int(os.getenv("RETENTION_DAYS", 7))
if RETENTION_DAYS <= 0:
    RETENTION_DAYS = 7

# ── Start date ────────────────────────────────────────────────────────
GENESIS_TIME = pd.to_datetime('2009-01-03 00:00:00').tz_localize(tz=timezone.utc)
START_DATE = os.getenv("START_DATE", None)
START_MONTH = None
if START_DATE:
    START_MONTH = START_DATE[:7]
    START_DATE = datetime.strptime(START_DATE, "%Y-%m-%d").date()
else:
    START_DATE = datetime(2009, 1, 3).date()

# ── Proxy ─────────────────────────────────────────────────────────────
proxy = os.getenv('PROXY_URL', '')

# ── WebSocket mode ────────────────────────────────────────────────────
ENABLE_WS = os.getenv("ENABLE_WS", "false").lower() == "true"

# ── Parquet file period (months) ──────────────────────────────────────
PARQUET_FILE_PERIOD = int(os.getenv('PARQUET_FILE_PERIOD', 1))

# ── loadhist → hot DuckDB (Plan A) ────────────────────────────────────
LOADHIST_FULL_HOT_LOAD = _env_bool("LOADHIST_FULL_HOT_LOAD", True)
LOADHIST_DELETE_PARQUET_AFTER_LOAD = _env_bool("LOADHIST_DELETE_PARQUET_AFTER_LOAD", False)

# ── hist download settings ───────────────────────────────────────────
RESOURCE_PATH = os.getenv("RESOURCE_PATH", "data/hist")
if not os.path.isabs(RESOURCE_PATH):
    RESOURCE_PATH = os.path.join(os.getcwd(), RESOURCE_PATH)

BASE_URL = 'https://data.binance.vision/'
root_center_url = 'https://s3-ap-northeast-1.amazonaws.com/data.binance.vision'

use_proxy_download_file = False
file_proxy = proxy if use_proxy_download_file else None

retry_times = 10
thunder = True
blind = False

metrics_prefix = None
SETTLED_SYMBOLS = None

def create_download_semaphore() -> asyncio.Semaphore:
    return asyncio.Semaphore(value=min(CONCURRENCY, 8))

need_analyse_set: set = set()
daily_updated_set: set = set()

SETTLED_USDT_PERP_SYMBOLS = {
    'ICPUSDT': ['2022-06-10 09:00:00', '2022-09-27 02:30:00'],
    'BNXUSDT': ['2023-02-11 04:00:00', '2023-02-22 22:45:00'],
    'TLMUSDT': ['2022-06-09 23:59:00', '2023-03-30 12:30:00'],
    'AERGOUSDT': ['2025-03-27 23:59:00', '2023-04-16 18:30:00'],
}

SETTLED_USDT_SPOT_SYMBOLS = {
    "BNXUSDT": ["2023-02-16 03:00:00", "2023-02-22 08:00:00"],
    "BTCSTUSDT": ["2021-03-15 07:00:00", "2021-03-19 07:00:00"],
    "COCOSUSDT": ["2021-01-19 02:00:00", "2021-01-23 02:00:00"],
    "CVCUSDT": ["2022-12-09 03:00:00", "2023-05-12 08:00:00"],
    "DREPUSDT": ["2021-03-29 04:00:00", "2021-04-02 04:00:00"],
    "FTTUSDT": ["2022-11-15 05:00:00", "2023-09-22 08:00:00"],
    "KEYUSDT": ["2023-02-10 03:00:00", "2023-03-10 08:00:00"],
    "LUNAUSDT": ["2022-05-13 01:00:00", "2022-05-31 06:00:00"],
    "QUICKUSDT": ["2023-07-17 03:00:00", "2023-07-21 08:00:00"],
    "STRAXUSDT": ["2024-03-20 03:00:00", "2024-03-28 08:00:00"],
    "SUNUSDT": ["2021-06-14 04:00:00", "2021-06-18 04:00:00"],
    "VIDTUSDT": ["2022-10-31 03:00:00", "2022-11-09 08:00:00"],
    "STX-USDT": ["2024-07-03 23:00:00", "2024-07-05 00:00:00"],
    "TIA-USDT": ["2024-07-03 23:00:00", "2024-07-05 00:00:00"],
    "VEN-USDT": ["2024-07-03 23:00:00", "2024-07-05 00:00:00"],
    "POND-USDT": ["2024-07-03 23:00:00", "2024-07-05 00:00:00"],
}

usdt_perp_delist_symbol_set = {
    '1000BTTCUSDT', 'CVCUSDT', 'DODOUSDT', 'RAYUSDT', 'SCUSDT', 'SRMUSDT',
    'LENDUSDT', 'NUUSDT', 'LUNAUSDT', 'YFIIUSDT', 'BTCSTUSDT',
}

usdt_spot_blacklist: list = []
