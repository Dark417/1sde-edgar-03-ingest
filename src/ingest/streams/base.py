"""L3: shared stream machinery — envelope construction, sink fan-out, universe.

The dual-sink rule lives here and only here: **the system of record commits
first, and every sink after it is a transport that may fail** (design doc §5.1).
Duplicating that rule per stream is how one stream eventually gets it backwards.

Does not handle: fetching (the client does) or path construction (contracts does).
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from edgar_lakehouse_contracts.envelope import LandingEnvelope
from edgar_lakehouse_contracts.names import Stream, batch_id, pad_cik

from ingest.logging import get_logger
from ingest.sinks.base import Sink, SinkResult

__all__ = [
    "StreamSummary",
    "build_envelope",
    "load_cik_universe",
    "write_to_sinks",
]

log = get_logger(__name__)

DEFAULT_UNIVERSE = Path(__file__).resolve().parent.parent / "data" / "cik_universe.json"


@dataclass
class StreamSummary:
    """The outcome of one stream run — what the ``ingest_complete`` line reports."""

    stream: Stream
    logical_date: date
    batch_id: str
    records: int = 0
    bytes_written: int = 0
    requests: int = 0
    sinks: list[str] = field(default_factory=list)
    landing_push_failed: bool = False

    def as_log_fields(self) -> dict[str, Any]:
        """Return the summary as the flat fields required by AGENTS.md §5.9."""
        return {
            "stream": str(self.stream.value),
            "logical_date": self.logical_date.isoformat(),
            "batch_id": self.batch_id,
            "records": self.records,
            "bytes": self.bytes_written,
            "requests": self.requests,
            "sinks": self.sinks,
            "landing_push_failed": self.landing_push_failed,
        }


def build_envelope(
    stream: Stream,
    logical_date: date,
    source_url: str,
    payload: dict[str, Any] | list[Any],
    resource_id: str,
    fetched_at: datetime | None = None,
    http_status: int = 200,
) -> LandingEnvelope:
    """Wrap a verbatim payload in a landing envelope.

    ``payload`` is passed through untouched: no date parsing, no case
    normalization, no dedup, no field renaming (AGENTS.md §5.6). Reshaping here
    would mean a replay from raw reproduces this repo's logic on the day it ran,
    rather than reproducing history.

    ``resource_id`` is the natural id of the thing fetched — an accession number, a
    padded CIK, or ``<padded cik>/<concept>``. Bronze dedupes on it, so it must be
    stable across re-fetches of the same resource and unique within a batch.

    ``http_status`` defaults to 200 because callers only reach this function once a
    payload has been successfully decoded; a non-200 never produces an envelope. It
    is explicit rather than hardcoded so a future partial-content or 304 path can
    record what actually happened.

    ``fetched_at`` is metadata only — never used in a key or filename
    (data contracts §1). ``content_sha256`` is derived by ``LandingEnvelope.build``.
    """
    return LandingEnvelope.build(
        stream=str(stream.value),
        resource_id=resource_id,
        logical_date=logical_date,
        batch_id=batch_id(stream, logical_date),
        fetched_at=fetched_at or datetime.now(UTC),
        request_url=source_url,
        http_status=http_status,
        payload=payload,
    )


def write_to_sinks(
    sinks: Sequence[Sink],
    stream: Stream,
    logical_date: date,
    records: Iterable[LandingEnvelope],
    summary: StreamSummary,
) -> None:
    """Write to every sink, first-commits-first, later sinks allowed to fail.

    The **first** sink is the system of record: its failure propagates and the
    CLI exits 3. Every subsequent sink is a transport — its failure logs
    ``LANDING_PUSH_FAILED`` at ERROR, sets a flag on the summary, and the run
    still exits 0. Ingest is never blocked by Databricks being down; getting
    this backwards defeats the entire replay story.

    ``records`` is materialized once because every sink must receive the same
    records, and a generator would be exhausted by the first.
    """
    materialized = list(records)
    if not sinks:
        return

    primary, *transports = sinks
    result = primary.write(stream, logical_date, materialized)
    summary.records = result.record_count
    summary.bytes_written = result.bytes_written
    summary.sinks.append(result.uri)

    for sink in transports:
        try:
            transport_result: SinkResult = sink.write(stream, logical_date, materialized)
            summary.sinks.append(transport_result.uri)
        except Exception as exc:
            summary.landing_push_failed = True
            log.error(
                "LANDING_PUSH_FAILED",
                sink=type(sink).__name__,
                stream=str(stream.value),
                logical_date=logical_date.isoformat(),
                error=str(exc),
                note="system of record committed; run continues and exits 0",
            )


def load_cik_universe(uri: str | None = None, limit: int | None = None) -> list[str]:
    """Return the CIK universe as zero-padded 10-character strings.

    Reads the packaged JSON list by default (the MVP1 source per AGENTS.md §6
    F-4); ``uri`` may point at any local JSON file exported from a Delta table.

    Accepts either a list of objects with a ``cik`` key or a bare list of CIKs,
    because a Delta export and a hand-written list do not look the same.

    Does not handle: fetching a universe over the network, or validating that
    each CIK exists in EDGAR.
    """
    path = Path(uri) if uri else DEFAULT_UNIVERSE
    if not path.exists():
        raise FileNotFoundError(f"CIK universe not found at {path}")

    raw: Any = json.loads(path.read_text())
    if not isinstance(raw, list):
        raise ValueError(f"CIK universe at {path} must be a JSON list, got {type(raw).__name__}")

    ciks: list[str] = []
    for entry in raw:
        value = entry.get("cik") if isinstance(entry, dict) else entry
        ciks.append(pad_cik(str(value)))

    # Sorted so the universe order is deterministic regardless of file order —
    # a resumable run must see the same sequence every time.
    ciks = sorted(set(ciks))
    return ciks[:limit] if limit is not None else ciks
