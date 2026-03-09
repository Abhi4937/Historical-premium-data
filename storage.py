"""
SQLite storage for OHLC data.

Schema (table: ohlc):
    timestamp  — UTC datetime of candle open (TEXT ISO format)
    symbol     — full contract symbol e.g. 'C-BTC-28MAR25-80000'
    expiry     — expiry tag e.g. '28MAR25'
    strike     — strike price e.g. 80000.0
    side       — 'call' or 'put'
    account    — account name from accounts.json
    open       — open price (premium in USD)
    high       — high price
    low        — low price
    close      — close price
    volume     — traded volume

Storage: single SQLite file — data/ohlc.db
    - One table: ohlc
    - Unique index on (timestamp, symbol) — prevents duplicates on re-run
    - Query example:
        SELECT * FROM ohlc WHERE expiry='28MAR25' AND strike=80000 AND side='call'
        SELECT * FROM ohlc WHERE symbol LIKE '%BTC%' AND timestamp > '2025-03-01'
"""

import logging
import sqlite3
import threading
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent / "data" / "ohlc.db"

# One lock per DB file — prevents concurrent writes from multiple threads
_db_lock = threading.Lock()

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS ohlc (
    timestamp  TEXT    NOT NULL,
    symbol     TEXT    NOT NULL,
    expiry     TEXT    NOT NULL,
    strike     REAL,
    side       TEXT,
    account    TEXT,
    open       REAL,
    high       REAL,
    low        REAL,
    close      REAL,
    volume     REAL,
    PRIMARY KEY (timestamp, symbol)
);
"""

CREATE_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_ohlc_symbol_time ON ohlc (symbol, timestamp);
CREATE INDEX IF NOT EXISTS idx_ohlc_expiry       ON ohlc (expiry);
CREATE INDEX IF NOT EXISTS idx_ohlc_strike_side  ON ohlc (strike, side);
"""


def _get_conn() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")   # allows concurrent reads during write
    conn.execute("PRAGMA synchronous=NORMAL") # faster writes, still safe
    return conn


def init_db() -> None:
    """Create table and indexes if they don't exist."""
    with _db_lock:
        conn = _get_conn()
        conn.execute(CREATE_TABLE_SQL)
        for stmt in CREATE_INDEX_SQL.strip().split("\n"):
            if stmt.strip():
                conn.execute(stmt.strip())
        conn.commit()
        conn.close()


def save_ohlc(df: pd.DataFrame, account: str, expiry: str, year: int, month: int) -> Path:
    """
    Save OHLC DataFrame to SQLite.
    Duplicate (timestamp, symbol) rows are silently ignored (INSERT OR IGNORE).
    Returns path to the DB file.
    """
    if df.empty:
        logger.warning("[%s] Empty DataFrame — nothing to save.", account)
        return None

    init_db()

    # Ensure timestamp is stored as ISO string
    df = df.copy()
    if pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
        df["timestamp"] = df["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    rows = df[["timestamp", "symbol", "expiry", "strike", "side", "account",
               "open", "high", "low", "close", "volume"]].values.tolist()

    with _db_lock:
        conn = _get_conn()
        conn.executemany(
            """INSERT OR IGNORE INTO ohlc
               (timestamp, symbol, expiry, strike, side, account,
                open, high, low, close, volume)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            rows,
        )
        conn.commit()
        inserted = conn.execute(
            "SELECT COUNT(*) FROM ohlc WHERE account=? AND expiry=? "
            "AND timestamp LIKE ?",
            (account, expiry, f"{year}-{month:02d}%"),
        ).fetchone()[0]
        conn.close()

    logger.info("[%s] Saved %d rows → %s  (DB total for %s %s-%02d: %d rows)",
                account, len(df), DB_PATH, expiry, year, month, inserted)
    return DB_PATH


def load_ohlc(
    expiry: str = None,
    strike: float = None,
    side: str = None,
    symbol: str = None,
    year: int = None,
    month: int = None,
) -> pd.DataFrame:
    """
    Query OHLC data from SQLite with optional filters.

    Examples:
        load_ohlc(expiry='28MAR25', strike=80000, side='call')
        load_ohlc(symbol='C-BTC-28MAR25-80000')
        load_ohlc(expiry='28MAR25', year=2025, month=3)
    """
    if not DB_PATH.exists():
        logger.warning("DB not found: %s", DB_PATH)
        return pd.DataFrame()

    conditions = []
    params = []

    if expiry:
        conditions.append("expiry = ?")
        params.append(expiry)
    if strike is not None:
        conditions.append("strike = ?")
        params.append(float(strike))
    if side:
        conditions.append("side = ?")
        params.append(side.lower())
    if symbol:
        conditions.append("symbol = ?")
        params.append(symbol)
    if year and month:
        conditions.append("timestamp LIKE ?")
        params.append(f"{year}-{month:02d}%")
    elif year:
        conditions.append("timestamp LIKE ?")
        params.append(f"{year}%")

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    sql = f"SELECT * FROM ohlc {where} ORDER BY symbol, timestamp"

    conn = _get_conn()
    df = pd.read_sql_query(sql, conn, params=params, parse_dates=["timestamp"])
    conn.close()
    return df
