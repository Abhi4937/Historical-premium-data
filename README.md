# Delta Exchange Historical Options OHLC Fetcher

Fetches historical 1-minute OHLC premium data for Delta Exchange options across multiple accounts in parallel.

## Features
- Auto-detects 4 expiries: weekly, current, next, next-to-next
- Filters ±20 strikes from ATM (call + put) per expiry
- Parallel execution — 1 thread per account (each has independent rate-limit quota)
- Work queue — accounts auto-pick next month when done
- SQLite storage with WAL mode (thread-safe, queryable)
- Exponential backoff on rate limits using `X-RATE-LIMIT-RESET` header

## Setup

```bash
# Clone and enter project
git clone https://github.com/Abhi4937/Historical-premium-data.git
cd Historical-premium-data

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Configure accounts
cp accounts.json.template accounts.json
# Edit accounts.json and fill in your API keys
```

## Usage

```bash
# Fetch last 15 months for BTC (auto-detects all 4 expiries)
python main.py --accounts accounts.json --underlying BTC --months 15

# Fetch specific expiry and month range
python main.py --accounts accounts.json --expiry 28MAR25 --underlying BTC --months 5

# Fetch a single month
python main.py --accounts accounts.json --underlying BTC --year 2025 --month 3
```

## Time estimates (5 accounts)

| Data range | Time |
|---|---|
| 1 day | ~1 min |
| 1 month | ~20 min |
| 5 months | ~20 min (parallel) |
| 15 months | ~41 min (parallel) |

## Data schema

Stored in `data/ohlc.db` (SQLite):

| Column | Description |
|---|---|
| timestamp | UTC candle open time |
| symbol | Full contract e.g. `C-BTC-28MAR25-80000` |
| expiry | Expiry tag e.g. `28MAR25` |
| strike | Strike price |
| side | `call` or `put` |
| account | Account name |
| open/high/low/close | Premium in USD |
| volume | Traded volume |

## Query data

```python
from storage import load_ohlc

# All BTC calls at strike 80000 for March expiry
df = load_ohlc(expiry='28MAR25', strike=80000, side='call')

# All data for a specific month
df = load_ohlc(expiry='28MAR25', year=2025, month=3)
```

## File structure

```
├── main.py              # CLI entry point, parallel orchestration
├── client.py            # API client, HMAC auth, rate-limit backoff
├── fetcher.py           # Product listing, ATM detection, candle fetching
├── storage.py           # SQLite save/load
├── accounts.json        # Your API keys (gitignored)
├── accounts.json.template
├── requirements.txt
└── data/
    └── ohlc.db          # SQLite database (gitignored)
```

## Notes
- Minimum resolution: `1m` (1-second candles not available on Delta Exchange API)
- Rate limit: 10,000 units / 5-min window per account; candles = 3 units/request
- Always keep `accounts.json` out of version control
