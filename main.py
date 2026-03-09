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
    5. Live terminal dashboard shows per-account progress, errors, timing

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

from datetime import timedelta
from dateutil.relativedelta import relativedelta

IST = timezone(timedelta(hours=5, minutes=30))

from client import DeltaClient
from fetcher import fetch_month_ohlc, get_expiries, fetch_spot_candles
from monitor import Monitor
from storage import save_ohlc, save_spot, init_db

# ── Logging — file only (rich handles terminal output) ────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("fetch.log", mode="a")],
)
logger = logging.getLogger(__name__)


def load_accounts(filepath: str) -> list[dict]:
    path = Path(filepath)
    if not path.exists():
        print(f"ERROR: Accounts file not found: {filepath}")
        print("Copy accounts.json.template to accounts.json and fill in your API keys.")
        sys.exit(1)
    with open(path) as f:
        accounts = json.load(f)
    valid = [
        a for a in accounts
        if a.get("api_key") and "YOUR_API" not in a.get("api_key", "")
    ]
    if not valid:
        print(f"ERROR: No valid API keys found in {filepath}")
        sys.exit(1)
    return valid


def build_month_queue(num_months: int, start_year: int = None, start_month: int = None) -> list[tuple]:
    now = datetime.now(tz=IST)
    dt = datetime(start_year or now.year, start_month or now.month, 1, tzinfo=IST)
    months = []
    for _ in range(num_months):
        months.append((dt.year, dt.month))
        dt -= relativedelta(months=1)
    return months


def worker(
    account: dict,
    month_q: queue.Queue,
    expiry_jobs: list[dict],
    resolution: str,
    monitor: Monitor,
    progress: dict,
    progress_lock: threading.Lock,
) -> None:
    name   = account["name"]
    client = DeltaClient(account["api_key"], account["api_secret"], account_name=name)
    state  = monitor.state[name]

    while True:
        try:
            year, month = month_q.get_nowait()
        except queue.Empty:
            state.set_done()
            monitor.log(f"[{name}] All months complete — worker finished", "ok")
            break

        # Peek at next month in queue
        try:
            next_ym = month_q.queue[0]
        except (IndexError, AttributeError):
            next_ym = None

        state.start_month(year, month, next_ym)
        monitor.log(f"[{name}] ▶ Started {year}-{month:02d}")

        # Step 1: fetch and save spot candles for this month (once per month, shared)
        spot_df = fetch_spot_candles(client, year, month)
        if not spot_df.empty:
            save_spot(spot_df, year, month)
            monitor.log(f"[{name}] Spot saved {year}-{month:02d} — {len(spot_df):,} rows", "ok")
        else:
            monitor.log(f"[{name}] ⚠ No spot data for {year}-{month:02d}", "warn")

        month_ok = True
        for job in expiry_jobs:
            expiry_tag = job["expiry_tag"]
            try:
                df = fetch_month_ohlc(
                    client=client,
                    expiry_date=expiry_tag,
                    year=year,
                    month=month,
                    resolution=resolution,
                    underlying=job.get("underlying"),
                    atm_strike=job.get("atm_strike"),
                    progress_callback=lambda done, total: state.update_progress(done, total),
                )

                if df.empty:
                    state.add_missing()
                    monitor.log(f"[{name}] ⚠ No data: {expiry_tag} {year}-{month:02d}", "warn")
                    month_ok = False
                    continue

                save_ohlc(df, name, expiry_tag, year, month)
                monitor.log(f"[{name}] ✅ Saved {expiry_tag} {year}-{month:02d} — {len(df):,} rows", "ok")

            except Exception as e:
                state.add_error()
                monitor.log(f"[{name}] ❌ Error {expiry_tag} {year}-{month:02d}: {e}", "error")
                logger.error("[%s] Error %s %s-%02d: %s", name, expiry_tag, year, month, e, exc_info=True)
                month_ok = False

        state.finish_month(year, month)

        with progress_lock:
            progress["done"] += 1
            done, total = progress["done"], progress["total"]

        status = "✅" if month_ok else "⚠ partial"
        monitor.log(f"[{name}] {status} {year}-{month:02d} done  [{done}/{total} months]", "ok" if month_ok else "warn")
        month_q.task_done()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Fetch historical OHLC option premium data — parallel multi-account.",
    )
    parser.add_argument("--accounts",   default="accounts.json")
    parser.add_argument("--underlying", default="BTC",
                        help="Underlying asset e.g. BTC, ETH (default: BTC)")
    parser.add_argument("--expiry",     default=None,
                        help="Force specific expiry tag e.g. '28MAR25'. Omit to auto-detect all 4.")
    parser.add_argument("--resolution", default="1m",
                        choices=["1m","3m","5m","15m","30m","1h","2h","4h","6h","12h","1d","1w"])

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--months", type=int,  help="Number of months going back from today")
    group.add_argument("--month",  type=int,  choices=range(1, 13), metavar="MONTH(1-12)")
    parser.add_argument("--year",  type=int,  help="Required with --month")

    return parser.parse_args()


def main():
    args = parse_args()
    if args.month and not args.year:
        print("ERROR: --year required with --month")
        sys.exit(1)

    accounts = load_accounts(args.accounts)
    init_db()

    # ── Step 1: detect expiries + ATM strikes ─────────────────────────────────
    probe = DeltaClient(accounts[0]["api_key"], accounts[0]["api_secret"],
                        account_name=accounts[0]["name"])

    if args.expiry:
        expiry_tags = {"manual": args.expiry}
    else:
        expiry_tags = get_expiries(probe, args.underlying)
        if not expiry_tags:
            print("ERROR: Could not auto-detect expiries. Use --expiry to set manually.")
            sys.exit(1)

    expiry_jobs = []
    for label, tag in expiry_tags.items():
        expiry_jobs.append({
            "label":      label,
            "expiry_tag": tag,
            "underlying": args.underlying,
        })

    # ── Step 2: build month queue ─────────────────────────────────────────────
    if args.months:
        months_list = build_month_queue(args.months)
    else:
        months_list = build_month_queue(1, args.year, args.month)

    total_months = len(months_list)
    num_workers  = min(len(accounts), total_months)

    # ── Step 3: start monitor dashboard ──────────────────────────────────────
    account_names = [a["name"] for a in accounts[:num_workers]]
    monitor = Monitor(account_names, months_list)
    monitor.log(f"Fetching {args.underlying} | expiries: {list(expiry_tags.values())} | {total_months} months | {num_workers} workers")
    monitor.start()

    # ── Step 4: fill queue and launch workers ─────────────────────────────────
    month_q: queue.Queue = queue.Queue()
    for ym in months_list:
        month_q.put(ym)

    progress      = {"done": 0, "total": total_months}
    progress_lock = threading.Lock()

    with ThreadPoolExecutor(max_workers=num_workers, thread_name_prefix="delta") as executor:
        futures = [
            executor.submit(
                worker,
                accounts[i],
                month_q,
                expiry_jobs,
                args.resolution,
                monitor,
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
    monitor.stop()
    monitor.print_summary()


if __name__ == "__main__":
    main()
