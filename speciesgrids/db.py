"""DuckDB helpers for speciesgrids builds."""

import logging
import os
import threading

import duckdb


logger = logging.getLogger(__name__)

_extensions_lock = threading.Lock()
_extensions_ready = False


def _ensure_extensions() -> None:
    """Install/load H3 + spatial once, without progress-bar noise."""
    global _extensions_ready
    if _extensions_ready:
        return
    with _extensions_lock:
        if _extensions_ready:
            return
        logger.info("[dim]Loading DuckDB extensions (h3, spatial)…[/dim]")
        con = duckdb.connect()
        con.execute("set enable_progress_bar=false")
        con.execute("install h3 from community")
        con.execute("load h3")
        con.execute("install spatial")
        con.execute("load spatial")
        con.close()
        _extensions_ready = True
        logger.info("[dim]DuckDB extensions ready[/dim]")


def connect(temp_directory: str | None = None) -> duckdb.DuckDBPyConnection:
    """Create a DuckDB connection with H3 + spatial loaded.

    DuckDB's own progress bar is left off — it is unlabeled and floods the
    terminal with instant 100% bars during INSTALL/LOAD. Long jobs use Rich
    spinners in the caller instead.
    """
    _ensure_extensions()
    con = duckdb.connect()
    if temp_directory is not None:
        os.makedirs(temp_directory, exist_ok=True)
        con.execute(f"set temp_directory='{temp_directory}'")
    con.execute("set enable_progress_bar=false")
    con.execute("load h3")
    con.execute("load spatial")
    return con
