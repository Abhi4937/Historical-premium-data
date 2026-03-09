"""
OHLC data fetcher for Delta Exchange options — Historical Replay architecture.

How it works:
  1. Fetch BTCUSD 1-min candles for the month → spot price at every minute
  2. For each minute find ATM = nearest strike to BTC spot price
  3. Collect ALL unique strikes needed across the whole month
     (BTC moves during month so ATM shifts — total unique strikes > 41)
  4. Fetch OHLC for all required strikes for that month
  5. Store spot data + options data together

Replay query example:
  At 14:00 IST → spot=68200 → ATM=68200 → filter ±20 strikes → option chain
  At 14:01 IST → spot=68350 → ATM=68500 → filter ±20 strikes → option chain

Endpoints used:
  GET /v2/history/candles  — OHLC candles (public, no auth)
  GET /v2/products         — option contracts list (public, no auth)
  GET /v2/tickers          — current spot price (public, no auth)
"""

import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Optional

import pandas as pd

from client import DeltaClient

logger = logging.getLogger(__name__)

IST = timezone(timedelta(hours=5, minutes=30))

RESOLUTION_MAP = {
    "1m": 1, "3m": 3, "5m": 5, "15m": 15, "30m": 30,
    "1h": 60, "2h": 120, "4h": 240, "6h": 360, "12h": 720,
    "1d": 1440, "1w": 10080,
}
DEFAULT_RESOLUTION = "1m"
MAX_CANDLES_PER_REQUEST = 2000
ATM_STRIKE_RANGE = 20


# ── Helpers ───────────────────────────────────────────────────────────────────

def _extract_expiry_tag(symbol: str) -> Optional[str]:
    """
    Extract expiry tag from symbol.
    Delta Exchange India format: 'C-BTC-69000-110326' → '110326' (DDMMYY)
    """
    parts = symbol.split("-")
    if len(parts) >= 4:
        tag = parts[-1]
        if len(tag) == 6 and tag.isdigit():
            return tag
    return None


def _nearest_strike(spot: float, strikes: list[float]) -> float:
    """Return the nearest available strike to the given spot price."""
    return min(strikes, key=lambda s: abs(s - spot))


# ── Spot price history ────────────────────────────────────────────────────────

def fetch_spot_candles(
    client: DeltaClient,
    year: int,
    month: int,
    resolution: str = "1m",
) -> pd.DataFrame:
    """
    Fetch BTCUSD 1-min candles for the given month.
    Used to get BTC spot price at every minute for ATM determination.

    Returns DataFrame: timestamp (IST), open, high, low, close, volume
    """
    from dateutil.relativedelta import relativedelta

    start_dt = datetime(year, month, 1, tzinfo=IST)
    end_dt   = start_dt + relativedelta(months=1)
    start_ts = int(start_dt.timestamp())
    end_ts   = int(end_dt.timestamp())

    logger.info("[%s] Fetching BTCUSD spot candles for %d-%02d", client.account_name, year, month)
    df = _fetch_candles_raw(client, "BTCUSD", start_ts, end_ts, resolution)
    if df.empty:
        logger.warning("[%s] No spot data for BTCUSD %d-%02d", client.account_name, year, month)
    else:
        logger.info("[%s] Got %d spot candles for %d-%02d", client.account_name, len(df), year, month)
    return df


# ── Expiry auto-detection ─────────────────────────────────────────────────────

def get_expiries(client: DeltaClient, underlying: str) -> dict:
    """
    Auto-detect 4 expiries: weekly, current, next, next_next.
    Returns dict: {'weekly': '090326', 'current': '270326', ...}
    """
    logger.info("[%s] Auto-detecting expiries for %s", client.account_name, underlying)

    resp = client.get("/v2/products", params={
        "contract_type": "call_options",
        "underlying_asset_symbol": underlying,
        "page_size": 200,
    })

    products = resp.get("result", [])
    now = datetime.now(tz=IST)

    expiry_dates = {}
    for p in products:
        symbol = p.get("symbol", "")
        tag = _extract_expiry_tag(symbol)
        if not tag:
            continue
        try:
            day   = int(tag[0:2])
            month = int(tag[2:4])
            year  = 2000 + int(tag[4:6])
            dt = datetime(year, month, day, 17, 30, 0, tzinfo=IST)  # 5:30 PM IST expiry
            if dt > now:
                expiry_dates[tag] = dt
        except Exception:
            continue

    if not expiry_dates:
        logger.warning("[%s] No future expiries found for %s", client.account_name, underlying)
        return {}

    sorted_expiries = sorted(expiry_dates.items(), key=lambda x: x[1])
    weeklies  = [(tag, dt) for tag, dt in sorted_expiries if dt.day < 25]
    monthlies = [(tag, dt) for tag, dt in sorted_expiries if dt.day >= 25]

    result = {}
    if weeklies:
        result["weekly"] = weeklies[0][0]
    if len(monthlies) >= 1:
        result["current"] = monthlies[0][0]
    if len(monthlies) >= 2:
        result["next"] = monthlies[1][0]
    if len(monthlies) >= 3:
        result["next_next"] = monthlies[2][0]

    logger.info("[%s] Expiries: %s", client.account_name, result)
    return result


# ── Strike selection based on historical spot ─────────────────────────────────

def get_required_strikes(
    spot_df: pd.DataFrame,
    available_strikes: list[float],
    n: int = ATM_STRIKE_RANGE,
) -> set[float]:
    """
    Given 1-min spot candles for a month, determine ALL unique strikes
    needed to cover ±n strikes from ATM at every minute.

    This accounts for BTC price movement during the month — the ATM
    shifts as price moves, so we need more than just 41 fixed strikes.

    Returns set of strike prices to fetch.
    """
    if spot_df.empty or not available_strikes:
        return set()

    sorted_strikes = sorted(available_strikes)
    required = set()

    for spot in spot_df["close"].dropna():
        atm = _nearest_strike(spot, sorted_strikes)
        atm_idx = sorted_strikes.index(atm)
        low_idx  = max(0, atm_idx - n)
        high_idx = min(len(sorted_strikes) - 1, atm_idx + n)
        for s in sorted_strikes[low_idx:high_idx + 1]:
            required.add(s)

    logger.info("Historical ATM range: spot %.0f–%.0f → %d unique strikes needed",
                spot_df["close"].min(), spot_df["close"].max(), len(required))
    return required


def get_all_strikes(
    client: DeltaClient,
    expiry_tag: str,
    underlying: str,
) -> list[float]:
    """Fetch all available strike prices for a given expiry."""
    params = {
        "contract_type": "call_options",
        "page_size": 200,
    }
    if underlying:
        params["underlying_asset_symbol"] = underlying

    all_strikes = []
    page = 1
    while True:
        params["page"] = page
        resp = client.get("/v2/products", params=params)
        products = resp.get("result", [])
        if not products:
            break
        for p in products:
            tag = _extract_expiry_tag(p.get("symbol", ""))
            if tag == expiry_tag and p.get("contract_type") == "call_options":
                try:
                    all_strikes.append(float(p["strike_price"]))
                except (KeyError, ValueError):
                    pass
        meta = resp.get("meta", {})
        if page >= meta.get("total_pages", 1):
            break
        page += 1
        time.sleep(0.2)

    return sorted(set(all_strikes))


# ── Candle fetching ───────────────────────────────────────────────────────────

def _fetch_candles_raw(
    client: DeltaClient,
    symbol: str,
    start_ts: int,
    end_ts: int,
    resolution: str = DEFAULT_RESOLUTION,
) -> pd.DataFrame:
    """Core candle fetcher — chunks time range to handle 2000 candle limit."""
    resolution_minutes = RESOLUTION_MAP.get(resolution, 1)
    all_candles = []
    chunk_seconds = MAX_CANDLES_PER_REQUEST * resolution_minutes * 60
    current_start = start_ts

    while current_start < end_ts:
        current_end = min(current_start + chunk_seconds, end_ts)
        try:
            resp = client.get("/v2/history/candles", params={
                "symbol": symbol,
                "resolution": resolution,
                "start": current_start,
                "end": current_end,
            })
            candles = resp.get("result", [])
            if isinstance(candles, dict):
                candles = candles.get("candles", [])
            if candles:
                all_candles.extend(candles)
        except Exception as e:
            logger.error("[%s] Error fetching %s: %s", client.account_name, symbol, e)

        current_start = current_end
        time.sleep(0.15)

    if not all_candles:
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])

    df = pd.DataFrame(all_candles)
    df.rename(columns={"time": "timestamp"}, inplace=True)
    df["timestamp"] = (pd.to_datetime(df["timestamp"], unit="s", utc=True)
                         .dt.tz_convert(IST))
    df = df.sort_values("timestamp").drop_duplicates("timestamp").reset_index(drop=True)
    for col in ["open", "high", "low", "close", "volume"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df[["timestamp", "open", "high", "low", "close", "volume"]]


# ── Month-level orchestration ─────────────────────────────────────────────────

def fetch_month_ohlc(
    client: DeltaClient,
    expiry_date: str,
    year: int,
    month: int,
    resolution: str = DEFAULT_RESOLUTION,
    underlying: Optional[str] = None,
    atm_strike: Optional[float] = None,   # ignored — kept for compatibility
    progress_callback=None,
) -> pd.DataFrame:
    """
    Fetch OHLC for all option contracts needed for historical replay:

    1. Fetch BTCUSD 1-min spot candles for the month
    2. Determine which strikes were within ±20 of ATM at any point during the month
    3. Fetch OHLC for all those strikes
    4. Return combined DataFrame

    This ensures correct ATM-based strike selection for every minute in history.
    """
    from dateutil.relativedelta import relativedelta

    start_dt = datetime(year, month, 1, tzinfo=IST)
    end_dt   = start_dt + relativedelta(months=1)
    start_ts = int(start_dt.timestamp())
    end_ts   = int(end_dt.timestamp())

    # Step 1: get BTC spot candles for this month
    spot_df = fetch_spot_candles(client, year, month, resolution)
    if spot_df.empty:
        logger.warning("[%s] No spot data — cannot determine historical ATM", client.account_name)
        return pd.DataFrame()

    # Step 2: get all available strikes for this expiry
    all_strikes = get_all_strikes(client, expiry_date, underlying)
    if not all_strikes:
        logger.warning("[%s] No strikes found for expiry %s", client.account_name, expiry_date)
        return pd.DataFrame()

    # Step 3: find all strikes needed across the entire month
    required_strikes = get_required_strikes(spot_df, all_strikes, ATM_STRIKE_RANGE)
    if not required_strikes:
        return pd.DataFrame()

    # Step 4: fetch call + put OHLC for each required strike
    # Build list of (symbol, strike, side) to fetch
    contracts = []
    for strike in sorted(required_strikes):
        # Construct symbol: C-BTC-STRIKE-EXPIRY and P-BTC-STRIKE-EXPIRY
        strike_int = int(strike)
        contracts.append((f"C-{underlying}-{strike_int}-{expiry_date}", strike, "call"))
        contracts.append((f"P-{underlying}-{strike_int}-{expiry_date}", strike, "put"))

    total = len(contracts)
    logger.info("[%s] Fetching %d contracts for %d-%02d (expiry %s)",
                client.account_name, total, year, month, expiry_date)

    all_frames = []
    for i, (symbol, strike, side) in enumerate(contracts, 1):
        if progress_callback:
            progress_callback(i, total)

        df = _fetch_candles_raw(client, symbol, start_ts, end_ts, resolution)
        if df.empty:
            logger.debug("[%s] No data for %s", client.account_name, symbol)
            continue

        df["symbol"]  = symbol
        df["expiry"]  = expiry_date
        df["strike"]  = strike
        df["side"]    = side
        df["account"] = client.account_name
        all_frames.append(df)

        logger.debug("[%s] (%d/%d) %s — %d candles", client.account_name, i, total, symbol, len(df))

    if not all_frames:
        return pd.DataFrame()

    combined = pd.concat(all_frames, ignore_index=True)
    return combined[["timestamp", "symbol", "expiry", "strike", "side", "account",
                      "open", "high", "low", "close", "volume"]]
