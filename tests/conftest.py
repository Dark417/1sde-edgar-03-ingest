"""Shared test fixtures.

**No test may hit the network** (AGENTS.md §5.11). Everything is served from
committed fixtures through respx; a test that reaches sec.gov is flaky and will
be deleted.
"""

from __future__ import annotations

import os
from collections.abc import Callable, Iterator
from datetime import date
from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures"

USER_AGENT = "edgar-lakehouse-test test@example.com"
LOGICAL_DATE = date(2026, 7, 29)

# The number of data rows in the committed index fixture.
FIXTURE_ROW_COUNT = 47


class FakeClock:
    """A monotonic clock that only advances when something sleeps.

    Lets the rate-limiter tests assert elapsed time exactly, without any test
    actually waiting.
    """

    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def index_text() -> str:
    """The committed real daily-index fixture."""
    return (FIXTURES / "form.20260729.idx").read_text()


@pytest.fixture
def submissions_json() -> str:
    return (FIXTURES / "submissions_CIK0000320193.json").read_text()


@pytest.fixture
def concept_json() -> str:
    return (FIXTURES / "companyconcept_CIK0000320193_Assets.json").read_text()


@pytest.fixture
def not_found_body() -> str:
    """The real 404 body — XML, not JSON, despite the .json URL."""
    return (FIXTURES / "companyconcept_404_body.xml").read_text()


@pytest.fixture
def universe_file(tmp_path: Path) -> Path:
    """A tiny CIK universe file."""
    path = tmp_path / "universe.json"
    path.write_text('[{"cik": "0000320193"}, {"cik": "0000789019"}, {"cik": "0001652044"}]')
    return path


@pytest.fixture(autouse=True)
def clean_env(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Strip every env var this project reads.

    Without this, a developer's exported SEC_USER_AGENT would make config tests
    pass locally and fail in CI, or vice versa.
    """
    for name in (
        "SEC_USER_AGENT",
        "LANDING_MODE",
        "RAW_BUCKET",
        "DBX_HOST",
        "DBX_TOKEN",
        "VOLUME_PATH",
        "MAX_RPS",
        "CIK_UNIVERSE_URI",
        "LOCAL_ONLY",
        "INGEST_LOCAL_ONLY",
        "LOCAL_LANDING_DIR",
    ):
        monkeypatch.delenv(name, raising=False)
    # moto and boto3 must never reach real AWS even if the runner has creds.
    for name, value in (
        ("AWS_ACCESS_KEY_ID", "testing"),
        ("AWS_SECRET_ACCESS_KEY", "testing"),
        ("AWS_SECURITY_TOKEN", "testing"),
        ("AWS_SESSION_TOKEN", "testing"),
        ("AWS_DEFAULT_REGION", "us-east-1"),
    ):
        monkeypatch.setenv(name, value)
    yield


@pytest.fixture
def make_envelopes() -> Callable[[int], list[object]]:
    """Build N envelopes with a frozen fetched_at, so bytes are comparable."""
    from datetime import UTC, datetime

    from edgar_lakehouse_contracts.names import Stream

    from ingest.streams.base import build_envelope

    def _make(count: int) -> list[object]:
        return [
            build_envelope(
                stream=Stream.FILING_INDEX,
                logical_date=LOGICAL_DATE,
                source_url="https://www.sec.gov/example",
                payload={"cik": f"{i:010d}", "form_type": "10-K"},
                fetched_at=datetime(2026, 8, 1, 12, 0, 0, tzinfo=UTC),
            )
            for i in range(count)
        ]

    return _make


@pytest.fixture(autouse=True)
def reset_logging() -> Iterator[None]:
    """Reset global logging state after every test.

    structlog and stdlib logging are process-global. A CLI test that runs under
    CliRunner leaves configuration behind that would otherwise leak into every
    later test in the session.
    """
    import structlog

    yield
    structlog.reset_defaults()


@pytest.fixture(autouse=True)
def no_ssm(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make SSM resolution a no-op by default.

    A unit test must not depend on whether the machine running it happens to
    have AWS credentials.
    """
    monkeypatch.setattr("ingest.config.resolve_ssm", lambda name: None)


def fixtures_dir() -> Path:
    return FIXTURES


assert os.path.isdir(FIXTURES), "fixtures directory is missing"
