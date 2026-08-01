"""L1: the EDGAR HTTP client — rate limiting, retries, URL construction.

The only component in the project that talks to the public internet.

Imports only ``edgar.errors``, ``edgar.parsers``, and the contracts package.
Does not handle: envelopes, sinks, or config resolution.
"""

from __future__ import annotations

import random
import time
from collections.abc import Callable, Iterator
from datetime import date
from typing import Any, Final

import httpx
from edgar_lakehouse_contracts.models import FilingIndexRecord
from edgar_lakehouse_contracts.names import pad_cik

from ingest.edgar.errors import FetchFailed, ForbiddenError, NoIndexForDate
from ingest.edgar.parsers import parse_form_index
from ingest.logging import get_logger

__all__ = ["ARCHIVES_BASE", "DATA_BASE", "MAX_RPS_HARD_CAP", "EdgarClient", "TokenBucket"]

ARCHIVES_BASE: Final[str] = "https://www.sec.gov"
DATA_BASE: Final[str] = "https://data.sec.gov"

MAX_RPS_HARD_CAP: Final[float] = 8.0
MAX_ATTEMPTS: Final[int] = 5
RETRY_STATUS: Final[frozenset[int]] = frozenset({429, 500, 502, 503, 504})
BACKOFF_BASE_S: Final[float] = 0.5

log = get_logger(__name__)


class TokenBucket:
    """A token bucket limiting sustained request rate to ``rate`` per second.

    A token bucket rather than ``sleep(1/rate)`` between calls: the sleep
    approach serializes at the wrong granularity, cannot absorb a burst, and
    silently slows down whenever the server is slow (because the sleep is
    additive to response time).

    The clock and sleep function are injected so the limiter is testable without
    spending wall-clock time.

    Does not handle: distributed rate limiting across concurrent tasks. This is a
    single-process batch job by design.
    """

    def __init__(
        self,
        rate: float,
        capacity: float | None = None,
        *,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if rate <= 0:
            raise ValueError(f"rate must be positive; got {rate}")
        self.rate = rate
        # Capacity 1 by default: no burst allowance. A bucket that can burst to
        # `rate` would let 5 requests leave simultaneously after any idle
        # second, which is exactly the shape that draws a 429. 20 requests at
        # 5 rps therefore take 19/5 = 3.8s, not 3.0s.
        self.capacity = capacity if capacity is not None else 1.0
        self._monotonic = monotonic
        self._sleep = sleep
        self._tokens = self.capacity
        self._updated_at = monotonic()

    def acquire(self, tokens: float = 1.0) -> None:
        """Block until ``tokens`` are available, then consume them.

        Single-pass rather than a refill loop. A loop that re-derives the token
        count from the clock after each sleep can spin forever: once the
        remaining deficit is small enough, the sleep interval rounds away when
        added to the clock, ``elapsed`` comes back as 0.0, and the deficit never
        shrinks. Sleeping exactly the deficit and crediting that many tokens
        directly is both terminating and easier to reason about.
        """
        now = self._monotonic()
        self._tokens = min(self.capacity, self._tokens + (now - self._updated_at) * self.rate)
        self._updated_at = now

        if self._tokens < tokens:
            self._sleep((tokens - self._tokens) / self.rate)
            self._updated_at = self._monotonic()
            self._tokens = tokens  # we slept exactly long enough to earn them
        self._tokens -= tokens


class EdgarClient:
    """Fetches from SEC EDGAR with a rate limit and a retry policy.

    Does not handle: wrapping results in envelopes, writing them anywhere, or
    knowing what a CIK universe is.
    """

    def __init__(
        self,
        user_agent: str,
        max_rps: float = 5.0,
        client: httpx.Client | None = None,
        *,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        """Validate the User-Agent and build the rate limiter.

        Raises at construction if the UA is missing an ``@``: the SEC 403s
        anonymous clients, and a 403 is never retried, so failing here with a
        clear message beats 403s at 06:00 UTC.
        """
        if not user_agent or "@" not in user_agent:
            raise ValueError(
                "user_agent must contain a contact email address (an '@'); "
                f"got {user_agent!r}. The SEC rejects anonymous clients."
            )
        if max_rps > MAX_RPS_HARD_CAP:
            raise ValueError(
                f"max_rps must not exceed the hard cap of {MAX_RPS_HARD_CAP}; got {max_rps}"
            )
        self.user_agent = user_agent
        self.max_rps = max_rps
        self._bucket = TokenBucket(max_rps, monotonic=monotonic, sleep=sleep)
        self._sleep = sleep
        self._client = client or httpx.Client(
            headers={"User-Agent": user_agent, "Accept-Encoding": "gzip, deflate"},
            timeout=httpx.Timeout(30.0),
            follow_redirects=True,
        )

    def close(self) -> None:
        """Close the underlying HTTP client."""
        self._client.close()

    def __enter__(self) -> EdgarClient:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    # ---------------------------------------------------------------- streams

    def daily_index_url(self, logical_date: date) -> str:
        """Return the daily form index URL for a date.

        The quarter is derived from the month; EDGAR partitions the daily index
        by quarter directory.
        """
        quarter = (logical_date.month - 1) // 3 + 1
        return (
            f"{ARCHIVES_BASE}/Archives/edgar/daily-index/{logical_date.year}"
            f"/QTR{quarter}/form.{logical_date.strftime('%Y%m%d')}.idx"
        )

    def fetch_daily_index(self, logical_date: date) -> Iterator[FilingIndexRecord]:
        """Yield the raw rows of the daily form index for ``logical_date``.

        Raises ``NoIndexForDate`` on 404 — weekends and market holidays have no
        index, and that is zero filings rather than a failure. The caller
        decides what that means (design doc §4.2.4).

        Raises ``IndexFormatChanged`` (from the parser) if the fixed-width
        layout no longer matches, without yielding any rows.
        """
        url = self.daily_index_url(logical_date)
        response = self._get(url)
        if response is None:
            raise NoIndexForDate(
                f"no daily index for {logical_date.isoformat()} ({url}) - "
                "weekend, market holiday, or the index is not published yet"
            )
        return parse_form_index(response.text)

    def submissions_url(self, cik: str) -> str:
        """Return the submissions document URL for a CIK."""
        return f"{DATA_BASE}/submissions/CIK{pad_cik(cik)}.json"

    def fetch_submissions(self, cik: str) -> dict[str, Any]:
        """Return the company submissions document, verbatim.

        Raises ``FetchFailed`` on 404: unlike a concept, a company in our
        universe not having a submissions document means the universe is wrong.

        Does not handle: flattening, exploding, or typing the document. It is
        deeply nested and its shape is not ours to control (data contracts §2.2).
        """
        url = self.submissions_url(cik)
        response = self._get(url)
        if response is None:
            raise FetchFailed(f"no submissions document for CIK {pad_cik(cik)} ({url})")
        payload: dict[str, Any] = response.json()
        return payload

    def company_concept_url(self, cik: str, taxonomy: str, concept: str) -> str:
        """Return the companyconcept URL for a (CIK, taxonomy, concept) triple."""
        return f"{DATA_BASE}/api/xbrl/companyconcept/CIK{pad_cik(cik)}/{taxonomy}/{concept}.json"

    def fetch_company_concept(self, cik: str, taxonomy: str, concept: str) -> dict[str, Any] | None:
        """Return the XBRL concept document, or ``None`` if the company does not report it.

        **A 404 here is not an error.** A company legitimately may not report a
        given concept — Apple, for instance, does not report
        ``us-gaap:CostOfRevenue``. Returns ``None``, logs at DEBUG, and the
        caller continues (design doc §4.2.5).

        Note the 404 body is XML, not JSON, despite the ``.json`` URL, so this
        decides on the status code and never parses a 404 body.
        """
        url = self.company_concept_url(cik, taxonomy, concept)
        response = self._get(url)
        if response is None:
            log.debug("concept_not_reported", cik=pad_cik(cik), taxonomy=taxonomy, concept=concept)
            return None
        payload: dict[str, Any] = response.json()
        return payload

    # ------------------------------------------------------------- transport

    def _get(self, url: str) -> httpx.Response | None:
        """GET with rate limiting and retries. Returns ``None`` on 404.

        Retries 429 and 5xx only, with exponential backoff plus jitter, up to
        ``MAX_ATTEMPTS``. Never retries 403 (the UA is wrong; retrying makes it
        worse) and never retries 404 (it will not become a 200).

        Does not handle: deciding whether a 404 is acceptable — that is the
        caller's, because it differs per stream.
        """
        last_error: Exception | None = None

        for attempt in range(1, MAX_ATTEMPTS + 1):
            self._bucket.acquire()
            try:
                response = self._client.get(url, headers={"User-Agent": self.user_agent})
            except httpx.HTTPError as exc:
                last_error = exc
                if attempt == MAX_ATTEMPTS:
                    break
                self._backoff(attempt, url=url, reason=type(exc).__name__)
                continue

            if response.status_code == 403:
                raise ForbiddenError(
                    f"EDGAR returned 403 for {url}. The User-Agent is missing or malformed "
                    f"(sent {self.user_agent!r}). Not retried - retrying a 403 does not fix "
                    "the User-Agent."
                )
            if response.status_code == 404:
                return None
            if response.status_code in RETRY_STATUS:
                last_error = FetchFailed(f"{response.status_code} from {url}")
                if attempt == MAX_ATTEMPTS:
                    break
                self._backoff(attempt, url=url, reason=str(response.status_code))
                continue
            if response.status_code >= 400:
                raise FetchFailed(f"unexpected {response.status_code} from {url}")
            return response

        raise FetchFailed(f"giving up on {url} after {MAX_ATTEMPTS} attempts: {last_error}")

    def _backoff(self, attempt: int, *, url: str, reason: str) -> None:
        """Sleep for an exponentially increasing, jittered interval.

        Jitter is not decoration: without it, every retrying client in a fleet
        wakes at the same instant and re-creates the burst that caused the 429.
        """
        delay = BACKOFF_BASE_S * (2 ** (attempt - 1))
        delay += random.uniform(0, delay / 2)
        log.warning("edgar_retry", url=url, attempt=attempt, reason=reason, delay_s=round(delay, 3))
        self._sleep(delay)
