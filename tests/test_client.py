"""The EDGAR client: rate limiting, retries, 404 semantics (F-2 acceptance).

Zero network: every request is served by respx from committed fixtures.
"""

from __future__ import annotations

import httpx
import pytest
import respx
from tests.conftest import LOGICAL_DATE, USER_AGENT, FakeClock

from ingest.edgar.client import MAX_RPS_HARD_CAP, EdgarClient, TokenBucket
from ingest.edgar.errors import FetchFailed, ForbiddenError, NoIndexForDate


def make_client(clock: FakeClock, max_rps: float = 5.0) -> EdgarClient:
    return EdgarClient(
        user_agent=USER_AGENT,
        max_rps=max_rps,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )


# ------------------------------------------------------------ construction


def test_user_agent_without_at_is_rejected_at_construction() -> None:
    """Fail at startup, not with 403s at 06:00 UTC (§5.1)."""
    with pytest.raises(ValueError, match="contact email"):
        EdgarClient(user_agent="edgar-lakehouse-demo")


def test_empty_user_agent_is_rejected() -> None:
    with pytest.raises(ValueError, match="contact email"):
        EdgarClient(user_agent="")


def test_max_rps_above_hard_cap_is_rejected() -> None:
    with pytest.raises(ValueError, match="hard cap"):
        EdgarClient(user_agent=USER_AGENT, max_rps=MAX_RPS_HARD_CAP + 0.1)


# ------------------------------------------------------------ rate limiting


def test_twenty_requests_at_5rps_take_at_least_3_8_seconds(clock: FakeClock) -> None:
    """F-2 acceptance, with a fake clock: no test spends real time."""
    bucket = TokenBucket(5.0, monotonic=clock.monotonic, sleep=clock.sleep)
    for _ in range(20):
        bucket.acquire()
    assert clock.now >= 3.8


def test_rate_limiter_does_not_over_sleep(clock: FakeClock) -> None:
    """5 rps must mean 5 rps, not 2."""
    bucket = TokenBucket(5.0, monotonic=clock.monotonic, sleep=clock.sleep)
    for _ in range(20):
        bucket.acquire()
    assert clock.now < 4.2


def test_bucket_refills_while_idle(clock: FakeClock) -> None:
    bucket = TokenBucket(5.0, monotonic=clock.monotonic, sleep=clock.sleep)
    bucket.acquire()
    clock.now += 10.0  # idle
    before = clock.now
    bucket.acquire()
    assert clock.now == before  # a refilled bucket does not sleep


def test_zero_rate_is_rejected() -> None:
    with pytest.raises(ValueError, match="positive"):
        TokenBucket(0)


# ------------------------------------------------------------ retry policy


@respx.mock
def test_403_is_not_retried(clock: FakeClock) -> None:
    """Exactly one request. Retrying a 403 does not fix the UA (§5.3)."""
    route = respx.get(url__startswith="https://www.sec.gov").mock(return_value=httpx.Response(403))
    with pytest.raises(ForbiddenError, match="403"):
        list(make_client(clock).fetch_daily_index(LOGICAL_DATE))
    assert route.call_count == 1


@respx.mock
def test_429_then_200_succeeds_in_two_requests(clock: FakeClock, index_text: str) -> None:
    route = respx.get(url__startswith="https://www.sec.gov").mock(
        side_effect=[httpx.Response(429), httpx.Response(200, text=index_text)]
    )
    records = list(make_client(clock).fetch_daily_index(LOGICAL_DATE))
    assert route.call_count == 2
    assert len(records) == 47


@respx.mock
def test_5xx_is_retried_then_gives_up(clock: FakeClock) -> None:
    route = respx.get(url__startswith="https://www.sec.gov").mock(return_value=httpx.Response(503))
    with pytest.raises(FetchFailed, match="after 5 attempts"):
        list(make_client(clock).fetch_daily_index(LOGICAL_DATE))
    assert route.call_count == 5


@respx.mock
def test_backoff_is_exponential(clock: FakeClock) -> None:
    respx.get(url__startswith="https://www.sec.gov").mock(return_value=httpx.Response(500))
    with pytest.raises(FetchFailed):
        list(make_client(clock).fetch_daily_index(LOGICAL_DATE))
    # sleeps include rate-limiter waits; the backoff ones grow
    backoffs = [s for s in clock.sleeps if s >= 0.5]
    assert len(backoffs) == 4
    assert backoffs == sorted(backoffs)
    assert backoffs[-1] > backoffs[0]


@respx.mock
def test_transport_error_is_retried(clock: FakeClock, index_text: str) -> None:
    route = respx.get(url__startswith="https://www.sec.gov").mock(
        side_effect=[httpx.ConnectError("boom"), httpx.Response(200, text=index_text)]
    )
    assert len(list(make_client(clock).fetch_daily_index(LOGICAL_DATE))) == 47
    assert route.call_count == 2


@respx.mock
def test_unexpected_4xx_is_not_retried(clock: FakeClock) -> None:
    route = respx.get(url__startswith="https://www.sec.gov").mock(return_value=httpx.Response(418))
    with pytest.raises(FetchFailed, match="unexpected 418"):
        list(make_client(clock).fetch_daily_index(LOGICAL_DATE))
    assert route.call_count == 1


# ------------------------------------------------------------ 404 semantics


@respx.mock
def test_weekend_404_raises_no_index_for_date(clock: FakeClock) -> None:
    """A Saturday has no filings; the caller decides that is success (§4.2.4)."""
    route = respx.get(url__startswith="https://www.sec.gov").mock(return_value=httpx.Response(404))
    with pytest.raises(NoIndexForDate):
        list(make_client(clock).fetch_daily_index(LOGICAL_DATE))
    assert route.call_count == 1  # a 404 is never retried


@respx.mock
def test_company_concept_404_returns_none(clock: FakeClock, not_found_body: str) -> None:
    """Apple genuinely does not report CostOfRevenue (§5.4).

    The body is XML despite the .json URL, so this also proves the client never
    parses a 404 body.
    """
    respx.get(url__startswith="https://data.sec.gov").mock(
        return_value=httpx.Response(404, text=not_found_body)
    )
    result = make_client(clock).fetch_company_concept("0000320193", "us-gaap", "CostOfRevenue")
    assert result is None


@respx.mock
def test_submissions_404_is_an_error(clock: FakeClock) -> None:
    """Unlike a concept, a missing submissions doc means the universe is wrong."""
    respx.get(url__startswith="https://data.sec.gov").mock(return_value=httpx.Response(404))
    with pytest.raises(FetchFailed, match="no submissions document"):
        make_client(clock).fetch_submissions("0000320193")


# ------------------------------------------------------------ URLs / parsing


@pytest.mark.parametrize(
    ("month", "quarter"),
    [(1, 1), (3, 1), (4, 2), (6, 2), (7, 3), (9, 3), (10, 4), (12, 4)],
)
def test_quarter_is_derived_from_month(clock: FakeClock, month: int, quarter: int) -> None:
    from datetime import date

    url = make_client(clock).daily_index_url(date(2026, month, 15))
    assert f"/QTR{quarter}/" in url


def test_daily_index_url_shape(clock: FakeClock) -> None:
    assert make_client(clock).daily_index_url(LOGICAL_DATE) == (
        "https://www.sec.gov/Archives/edgar/daily-index/2026/QTR3/form.20260729.idx"
    )


def test_urls_zero_pad_the_cik(clock: FakeClock) -> None:
    """cik is a STRING zero-padded to 10, everywhere (global law 4)."""
    client = make_client(clock)
    assert client.submissions_url("320193").endswith("CIK0000320193.json")
    assert "CIK0000320193" in client.company_concept_url("320193", "us-gaap", "Assets")


@respx.mock
def test_submissions_payload_is_verbatim(clock: FakeClock, submissions_json: str) -> None:
    import json

    respx.get(url__startswith="https://data.sec.gov").mock(
        return_value=httpx.Response(200, text=submissions_json)
    )
    assert make_client(clock).fetch_submissions("0000320193") == json.loads(submissions_json)


@respx.mock
def test_user_agent_header_is_sent(clock: FakeClock, index_text: str) -> None:
    route = respx.get(url__startswith="https://www.sec.gov").mock(
        return_value=httpx.Response(200, text=index_text)
    )
    list(make_client(clock).fetch_daily_index(LOGICAL_DATE))
    assert route.calls[0].request.headers["User-Agent"] == USER_AGENT


def test_client_is_a_context_manager() -> None:
    with EdgarClient(user_agent=USER_AGENT) as client:
        assert client.user_agent == USER_AGENT
