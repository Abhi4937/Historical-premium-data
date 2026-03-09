"""
Delta Exchange Options OHLC Fetcher — Parallel Multi-Account CLI

Usage:
    # Auto-detect all 4 expiries, last 15 months, 5 accounts in parallel
    python main.py --accounts accounts.json --underlying BTC --months 15

    # Specific expiry tag
    python main.py --accounts accounts.json --expiry 28MAR25 --underlying BTC --months 5

    # Single month
    python main.py --accounts accounts.json --underlying BTC --year 2025 --month 3

HOW IT WORKS:
    1. Auto-detects 4 expiries: weekly, current, next, next-next
    2. Gets current ATM strike per expiry, filters to ±20 strikes (call + put)
    3. Builds a work queue of months going back from today
    4. Spawns 1 thread per account — each pulls months from queue until empty
    5. Each account has its own rate-limit quota (per User ID) — true parallel

COMPLIANCE:
    - Official Delta Exchange endpoints only
    - 10,000 units/5-min per account; candles = 3 units; sleep 0.15s between requests
    - Exponential backoff on 429 using X-RATE-LIMIT-RESET header
    - Credentials always from accounts.json — never hardcoded
"""

import argparse
import json
import logging
import queue
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

from dateutil.relativedelta import relativedelta

from client import DeltaClient
from fetcher import fetch_month_ohlc, get_expiries, get_atm_strike
from storage import save_ohlc, init_db

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("fetch.log", mode="a"),
    ],
)
logger = logging.getLogger(__name__)


def load_accounts(filepath: str) -> list[dict]:
    path = Path(filepath)
    if not path.exists():
        logger.error("Accounts file not found: %s — copy accounts.json.template", filepath)
        sys.exit(1)
    with open(path) as f:
        accounts = json.load(f)
    valid = [
        a for a in accounts
        if a.get("api_key") and "YOUR_API" not in a.get("api_key", "")
    ]
    if not valid:
        logger.error("No valid API keys in %s", filepath)
        sys.exit(1)
    return valid


def build_month_queue(num_months: int, start_year: int = None, start_month: int = None) -> list[tuple]:
    """Build list of (year, month) going backwards from start date."""
    now = datetime.now(tz=timezone.utc)
    dt = datetime(start_year or now.year, start_month or now.month, 1, tzinfo=timezone.utc)
    months = []
    for _ in range(num_months):
        months.append((dt.year, dt.month))
        dt -= relativedelta(months=1)
    return months


def worker(
    account: dict,
    month_q: queue.Queue,
    expiry_jobs: list[dict],   # list of {expiry_tag, atm_strike}
    resolution: str,
    progress: dict,
    progress_lock: threading.Lock,
) -> None:
    """
    One worker per account thread.
    Pulls (year, month) from queue, fetches all expiries for that month, saves to DB.
    """
    name = account["name"]
    client = DeltaClient(account["api_key"], account["api_secret"], account_name=name)

    while True:
        try:
            year, month = month_q.get_nowait()
        except queue.Empty:
            logger.info("[%s] Queue empty — worker done.", name)
            break

        label = f"{year}-{month:02d}"
        logger.info("[%s] ▶ Starting %s (%d expiries)", name, label, len(expiry_jobs))

        for job in expiry_jobs:
            expiry_tag = job["expiry_tag"]
            atm_strike = job.get("atm_strike")
            try:
                df = fetch_month_ohlc(
                    client=client,
                    expiry_date=expiry_tag,
                    year=year,
                    month=month,
                    resolution=resolution,
                    underlying=job.get("underlying"),
                    atm_strike=atm_strike,
                )
                if df.empty:
                    logger.warning("[%s] No data for %s %s", name, expiry_tag, label)
                    continue
                save_ohlc(df, name, expiry_tag, year, month)
            except Exception as e:
                logger.error("[%s] Error %s %s: %s", name, expiry_tag, label, e, exc_info=True)

        with progress_lock:
            progress["done"] += 1
            done, total = progress["done"], progress["total"]
        logger.info("[%s] ✅ %s complete  [%d/%d months]", name, label, done, total)
        month_q.task_done()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Fetch historical OHLC option premium data — parallel multi-account.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--accounts", default="accounts.json")
    parser.add_argument("--underlying", default="BTC",
                        help="Underlying asset e.g. BTC, ETH (default: BTC)")
    parser.add_argument("--expiry", default=None,
                        help="Force a specific expiry tag e.g. '28MAR25'. "
                             "If omitted, auto-detects all 4 expiries.")
    parser.add_argument("--resolution", default="1m",
                        choices=["1m","3m","5m","15m","30m","1h","2h","4h","6h","12h","1d","1w"])

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--months", type=int,
                       help="Number of months going back from today e.g. 15")
    group.add_argument("--month", type=int, choices=range(1, 13), metavar="MONTH(1-12)")

    parser.add_argument("--year", type=int, help="Required with --month")
    return parser.parse_args()


def main():
    args = parse_args()
    if args.month and not args.year:
        logger.error("--year required with --month")
        sys.exit(1)

    accounts = load_accounts(args.accounts)
    init_db()

    # ── Step 1: detect expiries + ATM strikes (use first account) ────────────
    probe_client = DeltaClient(
        accounts[0]["api_key"], accounts[0]["api_secret"],
        account_name=accounts[0]["name"]
    )

    if args.expiry:
        expiry_tags = {"manual": args.expiry}
    else:
        expiry_tags = get_expiries(probe_client, args.underlying)
        if not expiry_tags:
            logger.error("Could not auto-detect expiries. Use --expiry to set manually.")
            sys.exit(1)

    # Build expiry jobs with ATM strikes
    expiry_jobs = []
    for label, tag in expiry_tags.items():
        atm = get_atm_strike(probe_client, args.underlying, tag)
        expiry_jobs.append({
            "label": label,
            "expiry_tag": tag,
            "atm_strike": atm,
            "underlying": args.underlying,
        })
        logger.info("  %-10s %s  ATM=%.0f  (±20 strikes = %d contracts)",
                    label, tag, atm or 0, 82 if atm else 0)

    # ── Step 2: build month queue ─────────────────────────────────────────────
    now = datetime.now(tz=timezone.utc)
    if args.months:
        months_list = build_month_queue(args.months)
    else:
        months_list = build_month_queue(1, args.year, args.month)

    total_months = len(months_list)
    num_workers = min(len(accounts), total_months)
    est_min = int((total_months / num_workers) * 20)

    logger.info("=" * 65)
    logger.info("Delta Exchange OHLC Fetcher")
    logger.info("  Underlying : %s", args.underlying)
    logger.info("  Expiries   : %s", list(expiry_tags.values()))
    logger.info("  Months     : %d  (%s → %s)",
                total_months,
                f"{months_list[-1][0]}-{months_list[-1][1]:02d}",
                f"{months_list[0][0]}-{months_list[0][1]:02d}")
    logger.info("  Workers    : %d accounts in parallel", num_workers)
    logger.info("  Est. time  : ~%d min", est_min)
    logger.info("  Storage    : %s", Path(__file__).parent / "data" / "ohlc.db")
    logger.info("=" * 65)

    # ── Step 3: fill queue and launch workers ─────────────────────────────────
    month_q: queue.Queue = queue.Queue()
    for ym in months_list:
        month_q.put(ym)

    progress = {"done": 0, "total": total_months}
    progress_lock = threading.Lock()

    with ThreadPoolExecutor(max_workers=num_workers, thread_name_prefix="delta") as executor:
        futures = [
            executor.submit(
                worker,
                accounts[i],
                month_q,
                expiry_jobs,
                args.resolution,
                progress,
                progress_lock,
            )
            for i in range(num_workers)
        ]
        for future in as_completed(futures):
            exc = future.exception()
            if exc:
                logger.error("Worker exception: %s", exc)

    month_q.join()
    logger.info("=" * 65)
    logger.info("All done. %d/%d months completed.", progress["done"], total_months)
    logger.info("Query your data: from storage import load_ohlc; df = load_ohlc(expiry='28MAR25')")
    logger.info("=" * 65)


if __name__ == "__main__":
    main()
