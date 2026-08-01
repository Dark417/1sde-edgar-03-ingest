"""L2: the local-filesystem sink.

Not part of AGENTS.md §6; see docs/03-ingest-design.md §8. It exists because the
current working mode is to pull data to local disk and load it into tables by
hand while repo 2's AWS infrastructure does not yet exist.

The path is taken from ``names.landing_path("volume", ...)`` and **re-rooted**,
never recomputed. Repo 1 stays the only place that knows how a landing path is
spelled, and the local tree is therefore layout-identical to the Volume tree —
so promoting it later is a straight copy with no path rewriting.

Does not handle: loading anything into a table. That is manual (docs/04).
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import date
from pathlib import Path

from edgar_lakehouse_contracts.envelope import LandingEnvelope
from edgar_lakehouse_contracts.names import VOLUME_LANDING, Stream, landing_path

from ingest.logging import get_logger
from ingest.sinks.base import SinkResult, encode_batch

__all__ = ["LocalSink"]

log = get_logger(__name__)


class LocalSink:
    """Writes a batch to ``{root}/edgar/{stream}/dt={date}/{batch_id}.json.gz``.

    Byte-identical to what the S3 and Volume sinks write for the same input —
    all three share ``encode_batch``. That identity is the point: the tables you
    hand-load from these files and the tables Auto Loader later builds from S3
    cannot disagree.
    """

    def __init__(self, root: str | Path) -> None:
        if not root:
            raise ValueError("root is required for LocalSink")
        self.root = Path(root)

    def target_path(self, stream: Stream, logical_date: date) -> Path:
        """Return the local path for a batch, re-rooted from the canonical layout."""
        canonical = landing_path("volume", stream, logical_date)
        relative = canonical.removeprefix(VOLUME_LANDING).lstrip("/")
        return self.root / "edgar" / relative

    def write(
        self, stream: Stream, logical_date: date, records: Iterable[LandingEnvelope]
    ) -> SinkResult:
        """Write the batch, creating parent directories as needed.

        Overwrites on re-run, exactly like the other sinks: the filename is
        deterministic in ``(stream, logical_date)``, so a second run of the same
        batch must replace the file rather than sit beside it.
        """
        batch = encode_batch(records)
        path = self.target_path(stream, logical_date)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(batch.data)

        log.info(
            "sink_write",
            sink="local",
            uri=str(path),
            bytes=len(batch.data),
            records=batch.record_count,
        )
        return SinkResult(
            uri=str(path), bytes_written=len(batch.data), record_count=batch.record_count
        )
