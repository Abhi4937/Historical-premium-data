# Delta Exchange OHLC Fetcher

## Project purpose
Fetch historical 1-min OHLC option premium data from Delta Exchange API across multiple accounts in parallel. Target: 15 months (Mar 2026 → Jan 2025), 4 expiries, ±20 strikes from ATM, call + put.

## Stack
- Python 3.12, venv at `./venv`
- Key libs: requests, pandas, python-dateutil
- Activate venv: `source venv/bin/activate`
- Run (last 15 months, 5 accounts parallel): `python main.py --accounts accounts.json --expiry 28MAR25 --months 15 --underlying BTC`
- Run (single month): `python main.py --accounts accounts.json --expiry 28MAR25 --year 2025 --month 3`

## File structure
- `main.py` — CLI, parallel ThreadPoolExecutor, work queue
- `client.py` — DeltaClient, HMAC-SHA256 auth, exponential backoff
- `fetcher.py` — product listing, candle fetching, month orchestration
- `storage.py` — SQLite save/load (data/ohlc.db), WAL mode, thread-safe writes
- `accounts.json` — credentials (gitignored, copy from accounts.json.template)
- `data/` — output files (gitignored)

## API details
- Base URL: `https://api.india.delta.exchange`
- Auth: HMAC-SHA256 — headers: `api-key`, `timestamp`, `signature`
- Candles endpoint: `GET /v2/history/candles?symbol=&resolution=&start=&end=`
- Products endpoint: `GET /v2/products?contract_type=put_options,call_options`
- Resolution format: `1m`, `3m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `6h`, `12h`, `1d`, `1w`
- Max 2000 candles per request
- Rate limit: 10,000 units / 5-min window (per user ID); candles = 3 units/request
- On 429: header `X-RATE-LIMIT-RESET` gives ms until reset

## Parallel design
- Rate limits are per User ID (authenticated) — each account has independent quota
- ThreadPoolExecutor: 1 thread per account
- Shared work queue: months distributed FIFO across accounts
- When an account finishes a month it auto-picks the next from queue
- 5 accounts × 15 months = 3 rounds × ~20 min = ~41 min total

## Data schema
timestamp, symbol, expiry, strike, side, account, open, high, low, close, volume

## Key constraints
- Sleep 0.15s between candle requests, 0.2s between product page requests
- Max 5 retries with exponential backoff on 429/5xx
- Never hardcode credentials — always use accounts.json
- 1-second candles NOT available — minimum is 1m

## Query examples (SQLite)
```python
from storage import load_ohlc
df = load_ohlc(expiry='28MAR25', strike=80000, side='call')
df = load_ohlc(expiry='28MAR25', year=2025, month=3)
```
