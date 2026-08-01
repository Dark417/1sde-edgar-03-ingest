"""Stream handlers (F-4 acceptance) — fan-out, resume, and the dual-sink rule."""

from __future__ import annotations

import gzip
import json
from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import Any

import httpx
import pytest
import respx
from edgar_lakehouse_contracts.concepts import CONCEPT_SET
from edgar_lakehouse_contracts.names import Stream
from tests.conftest import LOGICAL_DATE, USER_AGENT, FakeClock

from ingest.edgar.client import EdgarClient
from ingest.sinks.base import SinkResult
from ingest.sinks.local import LocalSink
from ingest.streams import company_concept, company_submissions, filing_index
from ingest.streams.base import StreamSummary, load_cik_universe, write_to_sinks


class RecordingSink:
    """A sink that counts calls and optionally fails."""

    def __init__(self, name: str = "rec", fail: bool = False) -> None:
        self.name = name
        self.fail = fail
        self.calls: list[int] = []

    def write(self, stream: Stream, logical_date: date, records: Any) -> SinkResult:
        materialized = list(records)
        self.calls.append(len(materialized))
        if self.fail:
            raise RuntimeError(f"{self.name} exploded")
        return SinkResult(uri=f"{self.name}://x", bytes_written=1, record_count=len(materialized))


def make_client(clock: FakeClock) -> EdgarClient:
    return EdgarClient(USER_AGENT, 5.0, monotonic=clock.monotonic, sleep=clock.sleep)


# ------------------------------------------------------------ filing_index


@respx.mock
def test_filing_index_lands_every_row(clock: FakeClock, index_text: str, tmp_path: Path) -> None:
    respx.get(url__startswith="https://www.sec.gov").mock(
        return_value=httpx.Response(200, text=index_text)
    )
    sink = LocalSink(tmp_path)
    summary = filing_index.run(make_client(clock), [sink], LOGICAL_DATE)

    assert summary.records == 47
    assert summary.requests == 1
    lines = gzip.decompress(Path(summary.sinks[0]).read_bytes()).decode().splitlines()
    assert len(lines) == 47


@respx.mock
def test_filing_index_payload_is_verbatim(
    clock: FakeClock, index_text: str, tmp_path: Path
) -> None:
    """§5.6: envelope fields only; nothing inside payload is touched."""
    respx.get(url__startswith="https://www.sec.gov").mock(
        return_value=httpx.Response(200, text=index_text)
    )
    summary = filing_index.run(make_client(clock), [LocalSink(tmp_path)], LOGICAL_DATE)
    first = json.loads(
        gzip.decompress(Path(summary.sinks[0]).read_bytes()).decode().splitlines()[0]
    )
    assert first["payload"] == {
        "company_name": "Hartley Opportunity Fund LLC",
        "form_type": "1-SA",
        "cik": "2056463",
        "date_filed": "20260729",
        "file_name": "edgar/data/2056463/0001096906-26-001138.txt",
    }
    assert first["_logical_date"] == "2026-07-29"
    assert first["_schema_version"] == "1"


@respx.mock
def test_weekend_is_success_with_zero_records(clock: FakeClock, tmp_path: Path) -> None:
    """A Saturday has no filings; that is not a failure (§4.2.4)."""
    respx.get(url__startswith="https://www.sec.gov").mock(return_value=httpx.Response(404))
    sink = RecordingSink()
    summary = filing_index.run(make_client(clock), [sink], date(2026, 8, 1))
    assert summary.records == 0
    assert sink.calls == []  # nothing written at all


# ------------------------------------------------------ company_submissions


@respx.mock
def test_submissions_respects_cik_limit(
    clock: FakeClock, submissions_json: str, tmp_path: Path, universe_file: Path
) -> None:
    route = respx.get(url__startswith="https://data.sec.gov").mock(
        return_value=httpx.Response(200, text=submissions_json)
    )
    summary = company_submissions.run(
        make_client(clock), [LocalSink(tmp_path)], LOGICAL_DATE, str(universe_file), cik_limit=2
    )
    assert route.call_count == 2
    assert summary.records == 2


@respx.mock
def test_submissions_skips_a_bad_cik_without_failing_the_run(
    clock: FakeClock, submissions_json: str, tmp_path: Path, universe_file: Path
) -> None:
    """One bad CIK must not cost the other 499."""
    respx.get(url__startswith="https://data.sec.gov").mock(
        side_effect=[
            httpx.Response(200, text=submissions_json),
            httpx.Response(404),
            httpx.Response(200, text=submissions_json),
        ]
    )
    summary = company_submissions.run(
        make_client(clock), [LocalSink(tmp_path)], LOGICAL_DATE, str(universe_file)
    )
    assert summary.requests == 3
    assert summary.records == 2


# --------------------------------------------------------- company_concept


@respx.mock
def test_concept_fanout_is_ciks_times_concept_set(
    clock: FakeClock, concept_json: str, tmp_path: Path, universe_file: Path
) -> None:
    """F-4 acceptance: --cik-limit 3 makes exactly 3 x len(CONCEPT_SET) requests."""
    route = respx.get(url__startswith="https://data.sec.gov").mock(
        return_value=httpx.Response(200, text=concept_json)
    )
    summary = company_concept.run(
        make_client(clock),
        [LocalSink(tmp_path)],
        LOGICAL_DATE,
        str(universe_file),
        cik_limit=3,
        checkpoint_dir=tmp_path / "ckpt",
    )
    assert route.call_count == 3 * len(CONCEPT_SET)
    assert summary.requests == 3 * len(CONCEPT_SET)


@respx.mock
def test_unreported_concepts_are_skipped_not_failed(
    clock: FakeClock, concept_json: str, not_found_body: str, tmp_path: Path, universe_file: Path
) -> None:
    """A 404 means the company does not report it — normal (§5.4)."""
    responses = []
    for i in range(len(CONCEPT_SET)):
        responses.append(
            httpx.Response(404, text=not_found_body)
            if i % 3 == 0
            else httpx.Response(200, text=concept_json)
        )
    respx.get(url__startswith="https://data.sec.gov").mock(side_effect=responses)

    summary = company_concept.run(
        make_client(clock),
        [LocalSink(tmp_path)],
        LOGICAL_DATE,
        str(universe_file),
        cik_limit=1,
        checkpoint_dir=tmp_path / "ckpt",
    )
    expected_404s = len([i for i in range(len(CONCEPT_SET)) if i % 3 == 0])
    assert summary.requests == len(CONCEPT_SET)
    assert summary.records == len(CONCEPT_SET) - expected_404s


@respx.mock
def test_resume_only_makes_the_remaining_requests(
    clock: FakeClock, concept_json: str, tmp_path: Path, universe_file: Path
) -> None:
    """F-4 acceptance: a crash at pair N resumes at N, not at zero."""
    checkpoint_dir = tmp_path / "ckpt"
    crash_after = 20

    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] > crash_after:
            raise RuntimeError("simulated crash")
        return httpx.Response(200, text=concept_json)

    respx.get(url__startswith="https://data.sec.gov").mock(side_effect=handler)

    with pytest.raises(Exception, match="simulated crash"):
        company_concept.run(
            make_client(clock),
            [LocalSink(tmp_path)],
            LOGICAL_DATE,
            str(universe_file),
            cik_limit=3,
            checkpoint_dir=checkpoint_dir,
        )

    assert company_concept.checkpoint_path(LOGICAL_DATE, checkpoint_dir).exists()

    # Resume: only the pairs that were never completed are refetched.
    respx.get(url__startswith="https://data.sec.gov").mock(
        return_value=httpx.Response(200, text=concept_json)
    )
    resumed = company_concept.run(
        make_client(clock),
        [LocalSink(tmp_path)],
        LOGICAL_DATE,
        str(universe_file),
        cik_limit=3,
        resume=True,
        checkpoint_dir=checkpoint_dir,
    )
    total = 3 * len(CONCEPT_SET)
    assert resumed.requests == total - crash_after
    assert resumed.records == total


@respx.mock
def test_checkpoint_is_removed_on_success(
    clock: FakeClock, concept_json: str, tmp_path: Path, universe_file: Path
) -> None:
    respx.get(url__startswith="https://data.sec.gov").mock(
        return_value=httpx.Response(200, text=concept_json)
    )
    checkpoint_dir = tmp_path / "ckpt"
    company_concept.run(
        make_client(clock),
        [LocalSink(tmp_path)],
        LOGICAL_DATE,
        str(universe_file),
        cik_limit=1,
        checkpoint_dir=checkpoint_dir,
    )
    assert not company_concept.checkpoint_path(LOGICAL_DATE, checkpoint_dir).exists()


@respx.mock
def test_a_stale_checkpoint_is_discarded_without_resume(
    clock: FakeClock, concept_json: str, tmp_path: Path, universe_file: Path
) -> None:
    """Without --resume the operator asked for a clean pass."""
    checkpoint_dir = tmp_path / "ckpt"
    path = company_concept.checkpoint_path(LOGICAL_DATE, checkpoint_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('{"cik": "0000320193", "concept": "Assets", "envelope": null}\n')

    route = respx.get(url__startswith="https://data.sec.gov").mock(
        return_value=httpx.Response(200, text=concept_json)
    )
    company_concept.run(
        make_client(clock),
        [LocalSink(tmp_path)],
        LOGICAL_DATE,
        str(universe_file),
        cik_limit=1,
        checkpoint_dir=checkpoint_dir,
    )
    assert route.call_count == len(CONCEPT_SET)  # nothing was skipped


@respx.mock
def test_truncated_checkpoint_line_is_survivable(
    clock: FakeClock, concept_json: str, tmp_path: Path, universe_file: Path
) -> None:
    """A half-written final line is the normal shape of a crash."""
    checkpoint_dir = tmp_path / "ckpt"
    path = company_concept.checkpoint_path(LOGICAL_DATE, checkpoint_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        '{"cik": "0000320193", "concept": "Assets", "envelope": null}\n{"cik": "000032019'
    )
    respx.get(url__startswith="https://data.sec.gov").mock(
        return_value=httpx.Response(200, text=concept_json)
    )
    summary = company_concept.run(
        make_client(clock),
        [LocalSink(tmp_path)],
        LOGICAL_DATE,
        str(universe_file),
        cik_limit=1,
        resume=True,
        checkpoint_dir=checkpoint_dir,
    )
    assert summary.requests == len(CONCEPT_SET) - 1  # only the intact pair was skipped


# ----------------------------------------------------------- dual-sink rule


def test_transport_failure_does_not_fail_the_run(
    make_envelopes: Callable[[int], list[Any]],
) -> None:
    """S3 commits first; a landing-push failure is logged and tolerated (§5.5)."""
    primary = RecordingSink("s3")
    transport = RecordingSink("volume", fail=True)
    summary = StreamSummary(Stream.FILING_INDEX, LOGICAL_DATE, "b")

    write_to_sinks(
        [primary, transport], Stream.FILING_INDEX, LOGICAL_DATE, make_envelopes(3), summary
    )

    assert summary.landing_push_failed is True
    assert summary.records == 3
    assert summary.sinks == ["s3://x"]  # the transport target is not claimed


def test_system_of_record_failure_propagates(
    make_envelopes: Callable[[int], list[Any]],
) -> None:
    """The first sink failing is fatal — the CLI maps it to exit 3."""
    summary = StreamSummary(Stream.FILING_INDEX, LOGICAL_DATE, "b")
    with pytest.raises(RuntimeError, match="exploded"):
        write_to_sinks(
            [RecordingSink("s3", fail=True)],
            Stream.FILING_INDEX,
            LOGICAL_DATE,
            make_envelopes(3),
            summary,
        )


def test_every_sink_receives_the_same_records(
    make_envelopes: Callable[[int], list[Any]],
) -> None:
    """A generator would be exhausted by the first sink."""
    sinks = [RecordingSink("a"), RecordingSink("b"), RecordingSink("c")]
    summary = StreamSummary(Stream.FILING_INDEX, LOGICAL_DATE, "b")
    write_to_sinks(sinks, Stream.FILING_INDEX, LOGICAL_DATE, iter(make_envelopes(5)), summary)
    assert [s.calls for s in sinks] == [[5], [5], [5]]


def test_no_sinks_is_a_no_op(make_envelopes: Callable[[int], list[Any]]) -> None:
    summary = StreamSummary(Stream.FILING_INDEX, LOGICAL_DATE, "b")
    write_to_sinks([], Stream.FILING_INDEX, LOGICAL_DATE, make_envelopes(2), summary)
    assert summary.records == 0


# ------------------------------------------------------------ CIK universe


def test_packaged_universe_loads_and_is_padded() -> None:
    ciks = load_cik_universe()
    assert len(ciks) == 25
    assert all(len(c) == 10 and c.isdigit() for c in ciks)


def test_universe_accepts_a_bare_list(tmp_path: Path) -> None:
    path = tmp_path / "u.json"
    path.write_text('["320193", "789019"]')
    assert load_cik_universe(str(path)) == ["0000320193", "0000789019"]


def test_universe_order_is_deterministic(tmp_path: Path) -> None:
    """A resumable run must see the same sequence every time."""
    path = tmp_path / "u.json"
    path.write_text('["789019", "320193", "320193"]')
    assert load_cik_universe(str(path)) == ["0000320193", "0000789019"]


def test_universe_limit_applies(tmp_path: Path) -> None:
    path = tmp_path / "u.json"
    path.write_text('["1", "2", "3", "4"]')
    assert len(load_cik_universe(str(path), limit=2)) == 2


def test_missing_universe_raises(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        load_cik_universe(str(tmp_path / "nope.json"))


def test_non_list_universe_raises(tmp_path: Path) -> None:
    path = tmp_path / "u.json"
    path.write_text('{"cik": "1"}')
    with pytest.raises(ValueError, match="must be a JSON list"):
        load_cik_universe(str(path))
