"""L2: the no-op sink, used by ``--dry-run``.

It still encodes the batch, so a dry run exercises serialization and reports a
truthful byte count — a dry run that skipped encoding would not tell you whether
the real run would succeed.

Does not handle: writing anything, anywhere. That is the entire specification.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import date
from typing import Literal

from edgar_lakehouse_contracts.envelope import LandingEnvelope
from edgar_lakehouse_contracts.names import Stream, landing_path

from ingest.logging import get_logger
from ingest.sinks.base import SinkResult, encode_batch

__all__ = ["NoopSink"]

log = get_logger(__name__)


class NoopSink:
    """Reports what would have been written, and writes nothing.

    ``--dry-run`` writing nothing is asserted by call-count tests, not by
    inspection (AGENTS.md §5.12) — hence there is a real class here rather than
    an ``if dry_run: return`` scattered through the stream handlers.
    """

    def __init__(
        self, mode: Literal["s3", "volume"] = "volume", raw_bucket: str = "dry-run-bucket"
    ) -> None:
        self.mode: Literal["s3", "volume"] = mode
        self.raw_bucket = raw_bucket

    def write(
        self, stream: Stream, logical_date: date, records: Iterable[LandingEnvelope]
    ) -> SinkResult:
        """Encode the batch, report the target, write nothing."""
        batch = encode_batch(records)
        uri = landing_path(self.mode, stream, logical_date, raw_bucket=self.raw_bucket)
        log.info(
            "sink_skipped",
            sink="noop",
            uri=uri,
            bytes=len(batch.data),
            records=batch.record_count,
            reason="dry_run",
        )
        return SinkResult(uri=uri, bytes_written=len(batch.data), record_count=batch.record_count)
