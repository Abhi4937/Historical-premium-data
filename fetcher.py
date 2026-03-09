"""
OHLC data fetcher for Delta Exchange options.

Notes on resolution:
  - Delta Exchange candles API resolution format: '1m','3m','5m','15m','30m','1h','2h','4h','6h','12h','1d','1w'
  - True 1-second OHLC is NOT available via the candles endpoint — minimum is 1-minute ('1m').
  - Max 2000 candles per API response.

Endpoints used:
  - GET /v2/products          — list all option contracts (public, no auth needed)
  - GET /v2/tickers           — current mark price for ATM detection
  - GET /v2/history/candles   — OHLC candles (public, no auth needed)
"""

import logging
import time
from datetime import datetime, timezone
from typing import Optional

import pandas as pd

from client import DeltaClient

logger = logging.getLogger(__name__)

RESOLUTION_MAP = {
    "1m": 1, "3m": 3, "5m": 5, "15m": 15, "30m": 30,
    "1h": 60, "2h": 120, "4h": 240, "6h": 360, "12h": 720,
    "1d": 1440, "1w": 10080,
}
DEFAULT_RESOLUTION = "1m"
MAX_CANDLES_PER_REQUEST = 2000
ATM_STRIKE_RANGE = 20  # ±20 strikes from ATM


# ── Expiry auto-detection ─────────────────────────────────────────────────────

def get_expiries(client: DeltaClient, underlying: str) -> dict:
    """
    Auto-detect the 4 relevant expiries for a given underlying:
      - weekly   : nearest Friday expiry
      - current  : nearest monthly expiry
      - next     : next monthly expiry
      - next_next: month after next

    Returns dict: {'weekly': 'DDMMMYY', 'current': ..., 'next': ..., 'next_next': ...}
    """
    logger.info("[%s] Auto-detecting expiries for %s", client.account_name, underlying)

    resp = client.get("/v2/products", params={
        "contract_type": "call_options",
        "underlying_asset_symbol": underlying,
        "page_size": 200,
    })

    products = resp.get("result", [])
    now = datetime.now(tz=timezone.utc)

    # Collect unique expiry dates
    expiry_dates = {}
    for p in products:
        expiry_str = p.get("settlement_time") or p.get("expiry_time", "")
        symbol = p.get("symbol", "")
        if not expiry_str:
            continue
        try:
            dt = datetime.fromisoformat(expiry_str.replace("Z", "+00:00"))
            if dt > now:
                # Extract the tag used in symbols e.g. '28MAR25'
                tag = _extract_expiry_tag(symbol)
                if tag:
                    expiry_dates[tag] = dt
        except Exception:
            continue

    if not expiry_dates:
        logger.warning("[%s] No future expiries found for %s", client.account_name, underlying)
        return {}

    # Sort by date
    sorted_expiries = sorted(expiry_dates.items(), key=lambda x: x[1])

    # Separate weeklies (expire on Fridays) from monthlies (last Friday of month)
    weeklies = []
    monthlies = []
    for tag, dt in sorted_expiries:
        # Monthly expiries are typically on the last Friday — day >= 25
        if dt.day >= 25:
            monthlies.append((tag, dt))
        else:
            weeklies.append((tag, dt))

    result = {}
    if weeklies:
        result["weekly"] = weeklies[0][0]
    if len(monthlies) >= 1:
        result["current"] = monthlies[0][0]
    if len(monthlies) >= 2:
        result["next"] = monthlies[1][0]
    if len(monthlies) >= 3:
        result["next_next"] = monthlies[2][0]

    logger.info("[%s] Expiries detected: %s", client.account_name, result)
    return result


def _extract_expiry_tag(symbol: str) -> Optional[str]:
    """Extract expiry tag from symbol e.g. 'C-BTC-28MAR25-80000' → '28MAR25'."""
    parts = symbol.split("-")
    # Format: C-BTC-28MAR25-80000 → parts[2] is the expiry
    for part in parts:
        if len(part) >= 5 and any(m in part for m in
                                   ["JAN","FEB","MAR","APR","MAY","JUN",
                                    "JUL","AUG","SEP","OCT","NOV","DEC"]):
            return part
    return None


# ── ATM detection ─────────────────────────────────────────────────────────────

def get_atm_strike(client: DeltaClient, underlying: str, expiry_tag: str) -> Optional[float]:
    """
    Get the current ATM strike by fetching the mark price of the underlying
    and finding the nearest available strike.
    """
    # Get underlying spot price via ticker
    resp = client.get("/v2/tickers", params={"contract_types": "spot"})
    tickers = resp.get("result", [])

    spot_price = None
    for t in tickers:
        if underlying.upper() in t.get("symbol", "").upper():
            spot_price = float(t.get("mark_price") or t.get("close", 0))
            break

    if not spot_price:
        logger.warning("[%s] Could not get spot price for %s", client.account_name, underlying)
        return None

    logger.info("[%s] %s spot price: %s", client.account_name, underlying, spot_price)

    # Get all strikes for this expiry and find nearest to spot
    resp = client.get("/v2/products", params={
        "contract_type": "call_options",
        "underlying_asset_symbol": underlying,
        "page_size": 200,
    })
    products = resp.get("result", [])

    strikes = []
    for p in products:
        tag = _extract_expiry_tag(p.get("symbol", ""))
        if tag == expiry_tag:
            try:
                strikes.append(float(p.get("strike_price", 0)))
            except (ValueError, TypeError):
                continue

    if not strikes:
        return None

    strikes = sorted(set(strikes))
    atm = min(strikes, key=lambda s: abs(s - spot_price))
    logger.info("[%s] ATM strike for %s %s: %s", client.account_name, underlying, expiry_tag, atm)
    return atm


def filter_atm_strikes(products: list[dict], atm_strike: float, n: int = ATM_STRIKE_RANGE) -> list[dict]:
    """
    Filter products to ±n strikes from ATM.
    Returns call and put contracts within the range.
    """
    all_strikes = sorted(set(
        float(p.get("strike_price", 0)) for p in products
        if p.get("strike_price")
    ))

    if atm_strike not in all_strikes:
        # Find nearest
        atm_strike = min(all_strikes, key=lambda s: abs(s - atm_strike))

    atm_idx = all_strikes.index(atm_strike)
    low_idx = max(0, atm_idx - n)
    high_idx = min(len(all_strikes) - 1, atm_idx + n)
    selected_strikes = set(all_strikes[low_idx:high_idx + 1])

    filtered = [p for p in products if float(p.get("strike_price", -1)) in selected_strikes]
    logger.info("ATM filter: %d strikes selected (ATM=%.0f ±%d) → %d contracts",
                len(selected_strikes), atm_strike, n, len(filtered))
    return filtered


# ── Product listing ───────────────────────────────────────────────────────────

def get_option_symbols(
    client: DeltaClient,
    expiry_tag: str,
    underlying: Optional[str] = None,
    atm_strike: Optional[float] = None,
    atm_range: int = ATM_STRIKE_RANGE,
) -> list[dict]:
    """
    Fetch all option contracts for a given expiry, optionally filtered to
    ±atm_range strikes from ATM.
    """
    logger.info("[%s] Fetching products for expiry=%s underlying=%s",
                client.account_name, expiry_tag, underlying or "all")

    params = {
        "contract_type": "put_options,call_options",
        "page_size": 100,
    }
    if underlying:
        params["underlying_asset_symbol"] = underlying

    all_products = []
    page = 1
    while True:
        params["page"] = page
        resp = client.get("/v2/products", params=params)
        products = resp.get("result", [])
        if not products:
            break

        matching = [
            p for p in products
            if expiry_tag.upper() in p.get("symbol", "").upper()
        ]
        all_products.extend(matching)

        meta = resp.get("meta", {})
        if page >= meta.get("total_pages", 1):
            break
        page += 1
        time.sleep(0.2)

    logger.info("[%s] Found %d contracts for expiry %s", client.account_name, len(all_products), expiry_tag)

    # Apply ±20 ATM filter if ATM strike is known
    if atm_strike and all_products:
        all_products = filter_atm_strikes(all_products, atm_strike, atm_range)

    return all_products


# ── Candle fetching ───────────────────────────────────────────────────────────

def fetch_candles(
    client: DeltaClient,
    symbol: str,
    start_ts: int,
    end_ts: int,
    resolution: str = DEFAULT_RESOLUTION,
) -> pd.DataFrame:
    """
    Fetch OHLC candles for a symbol between start_ts and end_ts (unix seconds).
    Chunks the time range to handle max 2000 candles per request.
    """
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
            candles = resp.get("result", {}).get("candles", [])
            if candles:
                all_candles.extend(candles)
        except Exception as e:
            logger.error("[%s] Error fetching candles for %s: %s", client.account_name, symbol, e)

        current_start = current_end
        time.sleep(0.15)

    if not all_candles:
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])

    df = pd.DataFrame(all_candles)
    df.rename(columns={"time": "timestamp"}, inplace=True)
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
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
    atm_strike: Optional[float] = None,
) -> pd.DataFrame:
    """
    Fetch OHLC for all option contracts of a given expiry across a full calendar month,
    filtered to ±20 strikes from ATM if atm_strike is provided.
    """
    from dateutil.relativedelta import relativedelta

    start_dt = datetime(year, month, 1, tzinfo=timezone.utc)
    end_dt = start_dt + relativedelta(months=1)
    start_ts = int(start_dt.timestamp())
    end_ts = int(end_dt.timestamp())

    products = get_option_symbols(
        client, expiry_date,
        underlying=underlying,
        atm_strike=atm_strike,
        atm_range=ATM_STRIKE_RANGE,
    )

    if not products:
        logger.warning("[%s] No products found for expiry=%s", client.account_name, expiry_date)
        return pd.DataFrame()

    all_frames = []
    for i, product in enumerate(products, 1):
        symbol = product["symbol"]
        strike = product.get("strike_price", "")
        side = "call" if "call" in product.get("contract_type", "").lower() else "put"

        logger.info("[%s] (%d/%d) %s", client.account_name, i, len(products), symbol)

        df = fetch_candles(client, symbol, start_ts, end_ts, resolution)
        if df.empty:
            continue

        df["symbol"] = symbol
        df["expiry"] = expiry_date
        df["strike"] = pd.to_numeric(strike, errors="coerce")
        df["side"] = side
        df["account"] = client.account_name
        all_frames.append(df)

    if not all_frames:
        return pd.DataFrame()

    combined = pd.concat(all_frames, ignore_index=True)
    return combined[["timestamp", "symbol", "expiry", "strike", "side", "account",
                      "open", "high", "low", "close", "volume"]]
