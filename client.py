"""
Delta Exchange API client with authentication and rate-limit handling.
"""

import hashlib
import hmac
import time
import logging
import requests
from typing import Optional

logger = logging.getLogger(__name__)

BASE_URL = "https://api.india.delta.exchange"

# Rate limit config
# Delta Exchange: 10,000 units per 5-min window; OHLC candles = 3 units/request
# On 429: response header X-RATE-LIMIT-RESET gives ms until reset
MAX_RETRIES = 5
BASE_BACKOFF = 2.0   # seconds
MAX_BACKOFF = 300.0  # seconds (up to 5-min window)


class DeltaClient:
    """Authenticated Delta Exchange API client with exponential backoff."""

    def __init__(self, api_key: str, api_secret: str, account_name: str = "default"):
        self.api_key = api_key
        self.api_secret = api_secret
        self.account_name = account_name
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})

    def _sign(self, method: str, path: str, query_string: str = "", body: str = "") -> dict:
        """Generate HMAC-SHA256 signature for Delta Exchange API."""
        timestamp = str(int(time.time()))
        message = method.upper() + timestamp + path + query_string + body
        signature = hmac.new(
            self.api_secret.encode("utf-8"),
            message.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        return {
            "api-key": self.api_key,
            "timestamp": timestamp,
            "signature": signature,
        }

    def get(self, path: str, params: Optional[dict] = None, authenticated: bool = False) -> dict:
        """
        Make a GET request with retry and exponential backoff on rate limits.

        Args:
            path: API path e.g. '/v2/history/candles'
            params: Query parameters dict
            authenticated: Whether to sign the request

        Returns:
            Parsed JSON response dict
        """
        url = BASE_URL + path
        query_string = ""
        if params:
            query_string = "?" + "&".join(f"{k}={v}" for k, v in sorted(params.items()))

        headers = {}
        if authenticated:
            headers = self._sign("GET", path, query_string)

        backoff = BASE_BACKOFF
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = self.session.get(url, params=params, headers=headers, timeout=30)

                if resp.status_code == 200:
                    return resp.json()

                elif resp.status_code == 429:
                    # X-RATE-LIMIT-RESET is in milliseconds until the window resets
                    reset_ms = resp.headers.get("X-RATE-LIMIT-RESET")
                    if reset_ms:
                        wait = min(float(reset_ms) / 1000.0 + 0.5, MAX_BACKOFF)
                    else:
                        wait = min(backoff, MAX_BACKOFF)
                    logger.warning(
                        "[%s] Rate limited (429). Waiting %.1fs (attempt %d/%d)",
                        self.account_name, wait, attempt, MAX_RETRIES,
                    )
                    time.sleep(wait)
                    backoff = min(backoff * 2, MAX_BACKOFF)

                elif resp.status_code in (500, 502, 503, 504):
                    logger.warning(
                        "[%s] Server error %d. Retrying in %.1fs (attempt %d/%d)",
                        self.account_name, resp.status_code, backoff, attempt, MAX_RETRIES,
                    )
                    time.sleep(backoff)
                    backoff = min(backoff * 2, MAX_BACKOFF)

                else:
                    logger.error(
                        "[%s] HTTP %d for %s: %s",
                        self.account_name, resp.status_code, path, resp.text[:200],
                    )
                    resp.raise_for_status()

            except requests.exceptions.ConnectionError as e:
                logger.warning("[%s] Connection error: %s. Retrying in %.1fs", self.account_name, e, backoff)
                time.sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF)

        raise RuntimeError(f"[{self.account_name}] Failed after {MAX_RETRIES} attempts for {path}")
