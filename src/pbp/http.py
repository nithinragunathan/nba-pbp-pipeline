"""Small HTTP helper. Retries belong to the Prefect tasks that call it (flows/pbp_flow.py), where
each attempt is visible in the UI; `is_retryable` tells those tasks which failures are worth
another attempt. Two exceptions stay here: a 429 that names its wait (`Retry-After`) is honoured
once, and a single large download (the NBA mirror) can ask for a few attempts."""
from __future__ import annotations

import logging
import time

import requests

log = logging.getLogger(__name__)
HEADERS = {"User-Agent": "Mozilla/5.0 (pbp-unify portfolio project)"}
MAX_RETRY_AFTER_S = 120


def is_retryable(exc: BaseException) -> bool:
    """Timeouts, dropped connections, 5xx and 429 can pass on another attempt; other errors (a
    404, a page without the data we parse) will not."""
    if isinstance(exc, (requests.Timeout, requests.ConnectionError)):
        return True
    if isinstance(exc, requests.HTTPError) and exc.response is not None:
        return exc.response.status_code == 429 or exc.response.status_code >= 500
    return False


def _retry_after_s(resp: requests.Response) -> int | None:
    value = resp.headers.get("Retry-After", "")
    return int(value) if value.isdigit() else None


def get(url: str, *, retries: int = 1, timeout: int = 45, stream: bool = False,
        headers: dict | None = None) -> requests.Response:
    delay = 2
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, timeout=timeout, stream=stream, headers={**HEADERS, **(headers or {})})
            wait = _retry_after_s(resp) if resp.status_code == 429 else None
            if wait is not None and wait <= MAX_RETRY_AFTER_S:
                log.warning("GET %s: 429, waiting %ds as asked", url, wait)
                time.sleep(wait)
                resp = requests.get(url, timeout=timeout, stream=stream, headers={**HEADERS, **(headers or {})})
            resp.raise_for_status()
            return resp
        except requests.RequestException as exc:
            if attempt == retries or not is_retryable(exc):
                raise
            log.warning("GET %s failed (%s); retry %d/%d in %ds", url, exc, attempt, retries - 1, delay)
            time.sleep(delay)
            delay *= 2
    raise AssertionError("unreachable")


def head(url: str, *, timeout: int = 30) -> requests.Response:
    resp = requests.head(url, timeout=timeout, headers=HEADERS, allow_redirects=True)
    resp.raise_for_status()
    return resp
