"""
Parquet storage for OHLC data, queried via DuckDB.

Why this combo:
  - Data saved as Parquet files → portable, shareable with other projects/tools
  - DuckDB used as query engine → fast SQL on top of Parquet, no server needed

File structure:
  data/parquet/
    {expiry}/
      {YYYY-MM}.parquet

  e.g. data/parquet/28MAR25/2025-03.parquet

Schema (Parquet columns):
    timestamp  — UTC datetime
    symbol     — e.g. 'C-BTC-28MAR25-80000'
    expiry     — e.g. '28MAR25'
    strike     — float e.g. 80000.0
    side       — 'call' or 'put'
    account    — account name
    open       — premium open
    high       — premium high
    low        — premium low
    close      — premium close
    volume     — traded volume

Query examples:
    from storage import query
    df = query("SELECT * FROM ohlc WHERE strike=80000 AND side='call'")
    df = query("SELECT * FROM ohlc WHERE expiry='28MAR25' AND timestamp > '2025-03-01'")
    df = query("SELECT strike, AVG(close) FROM ohlc GROUP BY strike ORDER BY strike")
"""

import logging
import threading
from pathlib import Path

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)

DATA_DIR  = Path(__file__).parent / "data" / "parquet"
# DuckDB reads all parquet files under DATA_DIR with a glob
PARQUET_GLOB = str(DATA_DIR / "**" / "*.parquet")

_write_lock = threading.Lock()   # one write at a time across threads


def save_ohlc(df: pd.DataFrame, account: str, expiry: str, year: int, month: int) -> Path:
    """
    Save OHLC DataFrame as a Parquet file.
    If the file already exists, merges and deduplicates on (timestamp, symbol).

    Returns path to the saved Parquet file.
    """
    if df.empty:
        logger.warning("[%s] Empty DataFrame — nothing to save.", account)
        return None

    out_dir = DATA_DIR / expiry
    out_dir.mkdir(parents=True, exist_ok=True)
    filepath = out_dir / f"{year}-{month:02d}.parquet"

    # Ensure timestamp is proper datetime
    df = df.copy()
    if not pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

    with _write_lock:
        if filepath.exists():
            existing = pd.read_parquet(filepath)
            combined = pd.concat([existing, df], ignore_index=True)
            combined = (combined
                        .drop_duplicates(subset=["timestamp", "symbol"])
                        .sort_values(["symbol", "timestamp"])
                        .reset_index(drop=True))
            combined.to_parquet(filepath, index=False)
            logger.info("[%s] Updated %s  (+%d rows, total %d)",
                        account, filepath, len(df), len(combined))
        else:
            df_sorted = df.sort_values(["symbol", "timestamp"]).reset_index(drop=True)
            df_sorted.to_parquet(filepath, index=False)
            logger.info("[%s] Saved %s  (%d rows)", account, filepath, len(df_sorted))

    return filepath


def query(sql: str) -> pd.DataFrame:
    """
    Run a SQL query across ALL Parquet files in data/parquet/.
    The table name to use in SQL is 'ohlc'.

    Args:
        sql: SQL string using 'ohlc' as the table name

    Returns:
        pandas DataFrame

    Examples:
        query("SELECT * FROM ohlc WHERE strike=80000 AND side='call'")
        query("SELECT expiry, COUNT(*) as rows FROM ohlc GROUP BY expiry")
        query("SELECT * FROM ohlc WHERE timestamp > '2025-03-01' LIMIT 100")
    """
    if not DATA_DIR.exists() or not any(DATA_DIR.rglob("*.parquet")):
        logger.warning("No Parquet files found in %s", DATA_DIR)
        return pd.DataFrame()

    con = duckdb.connect()
    # Register all parquet files as a virtual table called 'ohlc'
    con.execute(f"CREATE VIEW ohlc AS SELECT * FROM read_parquet('{PARQUET_GLOB}')")
    result = con.execute(sql).df()
    con.close()
    return result


def load_ohlc(
    expiry: str = None,
    strike: float = None,
    side: str = None,
    year: int = None,
    month: int = None,
) -> pd.DataFrame:
    """
    Convenience wrapper around query() with common filters.

    Examples:
        load_ohlc(expiry='28MAR25', strike=80000, side='call')
        load_ohlc(expiry='28MAR25', year=2025, month=3)
    """
    conditions = []
    if expiry:
        conditions.append(f"expiry = '{expiry}'")
    if strike is not None:
        conditions.append(f"strike = {float(strike)}")
    if side:
        conditions.append(f"side = '{side.lower()}'")
    if year and month:
        conditions.append(f"timestamp >= '{year}-{month:02d}-01' "
                          f"AND timestamp < '{year}-{month:02d}-01'::DATE + INTERVAL 1 MONTH")
    elif year:
        conditions.append(f"YEAR(timestamp) = {year}")

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    return query(f"SELECT * FROM ohlc {where} ORDER BY symbol, timestamp")


def init_db() -> None:
    """Create data directory — no DB server to initialise for Parquet."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    logger.info("Parquet storage ready at %s", DATA_DIR)
