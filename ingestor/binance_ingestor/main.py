#!/usr/bin/env python3
"""
Binance Ingestor — standalone entry point.

Connects to duckport-rs, initialises the schema, then runs the data
fetching loop (REST or WebSocket mode).

Usage:
    python -m binance_ingestor.main
    # or via pyproject.toml script entry:
    binance-ingestor
"""

import os
import sys
import signal

from binance_ingestor.utils.log_kit import logger, divider

os.environ['TZ'] = 'UTC'


def main():
    from binance_ingestor.config import (
        DUCKPORT_ADDR, DUCKPORT_SCHEMA,
        KLINE_INTERVAL, DATA_SOURCES, ENABLE_WS,
    )
    from binance_ingestor.duckport_client import DuckportClient
    from binance_ingestor.data_jobs import (
        RestfulDataJobs, WebsocketsDataJobs, KLINE_MARKETS,
    )

    divider("Binance Ingestor starting")

    client = DuckportClient(addr=DUCKPORT_ADDR, schema=DUCKPORT_SCHEMA)

    def signal_handler(signum, frame):
        logger.critical("Received exit signal, shutting down...")
        client.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        info = client.ping()
        logger.info(f"duckport-rs server: {info}")
    except Exception as e:
        logger.error(f"Cannot reach duckport-rs at {DUCKPORT_ADDR}: {e}")
        sys.exit(1)

    logger.info(f"Initialising schema on duckport-rs (markets={list(DATA_SOURCES)}, interval={KLINE_INTERVAL})")
    client.init_schema(
        markets=list(KLINE_MARKETS),
        interval=KLINE_INTERVAL,
        data_sources=DATA_SOURCES,
    )
    client.verify_kline_interval(KLINE_INTERVAL)

    if ENABLE_WS:
        WebsocketsDataJobs(client)
    else:
        RestfulDataJobs(client)

    # DataJobs spawns daemon threads; keep main thread alive
    try:
        signal.pause()
    except AttributeError:
        # Windows: signal.pause() not available
        import threading
        threading.Event().wait()


if __name__ == "__main__":
    main()
