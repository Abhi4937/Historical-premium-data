"""
Parquet storage for OHLC data + BTCUSD spot data.

File structure:
  data/
    parquet/
      {expiry}/
        {YYYY-MM}.parquet   ← options OHLC per expiry per month
    spot/
      {YYYY-MM}.parquet     ← BTCUSD 1-min spot candles per month

Options schema:
    timestamp  — IST datetime (1-min candle)
    symbol     — e.g. 'C-BTC-69000-010325'
    expiry     — e.g. '010325' (DDMMYY)
    strike     — float e.g. 69000.0
    side       — 'call' or 'put'
    account    — account name
    open/high/low/close  — premium in USD
    volume     — contracts traded

Spot schema:
    timestamp  — IST datetime
    open/high/low/close  — BTCUSD price
    volume     — volume
"""

import logging
import threading
from pathlib import Path

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)

DATA_DIR     = Path(__file__).parent / "data" / "parquet"
SPOT_DIR     = Path(__file__).parent / "data" / "spot"
PARQUET_GLOB = str(DATA_DIR / "**" / "*.parquet")

_write_lock = threading.Lock()


def save_ohlc(df: pd.DataFrame, account: str, expiry: str, year: int, month: int) -> Path:
    """Save options OHLC DataFrame as Parquet. Merges if file already exists."""
    if df.empty:
        logger.warning("[%s] Empty DataFrame — nothing to save.", account)
        return None

    out_dir = DATA_DIR / expiry
    out_dir.mkdir(parents=True, exist_ok=True)
    filepath = out_dir / f"{year}-{month:02d}.parquet"

    df = df.copy()
    if not pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

    with _write_lock:
        if filepath.exists():
            existing = pd.read_parquet(filepath)
            combined = (pd.concat([existing, df], ignore_index=True)
                        .drop_duplicates(subset=["timestamp", "symbol"])
                        .sort_values(["symbol", "timestamp"])
                        .reset_index(drop=True))
            combined.to_parquet(filepath, index=False)
            logger.info("[%s] Updated %s (+%d rows, total %d)", account, filepath, len(df), len(combined))
        else:
            df.sort_values(["symbol", "timestamp"]).to_parquet(filepath, index=False)
            logger.info("[%s] Saved %s (%d rows)", account, filepath, len(df))

    return filepath


def save_spot(df: pd.DataFrame, year: int, month: int) -> Path:
    """Save BTCUSD 1-min spot candles as Parquet."""
    if df.empty:
        return None

    SPOT_DIR.mkdir(parents=True, exist_ok=True)
    filepath = SPOT_DIR / f"{year}-{month:02d}.parquet"

    df = df.copy()
    if not pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

    with _write_lock:
        if filepath.exists():
            existing = pd.read_parquet(filepath)
            combined = (pd.concat([existing, df], ignore_index=True)
                        .drop_duplicates(subset=["timestamp"])
                        .sort_values("timestamp")
                        .reset_index(drop=True))
            combined.to_parquet(filepath, index=False)
        else:
            df.sort_values("timestamp").to_parquet(filepath, index=False)

    logger.info("Spot saved → %s (%d rows)", filepath, len(df))
    return filepath


def load_spot(year: int, month: int) -> pd.DataFrame:
    """Load BTCUSD spot candles for a given month."""
    filepath = SPOT_DIR / f"{year}-{month:02d}.parquet"
    if not filepath.exists():
        return pd.DataFrame()
    return pd.read_parquet(filepath)


def query(sql: str) -> pd.DataFrame:
    """Run SQL across all options Parquet files. Table name: 'ohlc'."""
    if not DATA_DIR.exists() or not any(DATA_DIR.rglob("*.parquet")):
        logger.warning("No Parquet files found in %s", DATA_DIR)
        return pd.DataFrame()

    con = duckdb.connect()
    con.execute(f"CREATE VIEW ohlc AS SELECT * FROM read_parquet('{PARQUET_GLOB}')")
    result = con.execute(sql).df()
    con.close()
    return result


def init_db() -> None:
    """Create data directories."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    SPOT_DIR.mkdir(parents=True, exist_ok=True)
    logger.info("Storage ready — options: %s  spot: %s", DATA_DIR, SPOT_DIR)
