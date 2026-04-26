#!/usr/bin/env python3
"""
Convenience launcher — equivalent to ``python -m binance_ingestor.main``
or the ``binance-ingestor`` CLI entry point.

Replaces the old ``duckport/start_server.py`` which started the Python
FlightServer + local DuckDB.  This script only needs duckport-rs running.
"""

from binance_ingestor.main import main

if __name__ == "__main__":
    main()
