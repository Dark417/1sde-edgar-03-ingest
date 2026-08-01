"""L1: the Sink protocol, its result type, and the one shared encoder.

Every sink writes the *identical* bytes object produced by ``encode_batch``.
Byte-identity between sinks is therefore not something each sink tries to
achieve — it is something none of them can avoid. That property is what makes
an S3 replay reproduce exactly what the live Volume path produced (design doc
§5.1); if the sinks could diverge, you would not find out until you needed the
replay.

Imports only the contracts package. Does not handle: any I/O.
"""

from __future__ import annotations

import gzip
import io
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date
from typing import Protocol, runtime_checkable

from edgar_lakehouse_contracts.envelope import LandingEnvelope
from edgar_lakehouse_contracts.names import Stream

__all__ = ["EncodedBatch", "Sink", "SinkResult", "encode_batch"]


@dataclass(frozen=True)
class SinkResult:
    """What a sink wrote."""

    uri: str
    bytes_written: int
    record_count: int


@dataclass(frozen=True)
class EncodedBatch:
    """One batch, already serialized to the exact bytes every sink will write."""

    data: bytes
    record_count: int

    def reader(self) -> io.BytesIO:
        """Return a fresh file-like reader over the bytes.

        Sinks that stream (the Files API upload) need a reader they can consume
        without consuming it for the next sink.
        """
        return io.BytesIO(self.data)


@runtime_checkable
class Sink(Protocol):
    """Writes one batch of envelopes to one destination."""

    def write(
        self, stream: Stream, logical_date: date, records: Iterable[LandingEnvelope]
    ) -> SinkResult:
        """Write the batch and return what was written."""
        ...


def encode_batch(records: Iterable[LandingEnvelope]) -> EncodedBatch:
    """Serialize envelopes to gzip NDJSON: one envelope per line.

    ``mtime=0`` is load-bearing. The default gzip header embeds the current
    time, so the same batch encoded twice would produce different bytes and
    quietly break both the byte-identity property and the "run it twice"
    guarantee. Object *keys* are deterministic via ``names.batch_id``; the
    contents have to be too.

    ``compresslevel`` is pinned for the same reason — the default is stable
    today but is not a documented guarantee.

    Does not handle: writing the bytes anywhere, or bounding memory for very
    large batches (a day of filings is ~1 MB gzipped).
    """
    buffer = io.BytesIO()
    count = 0
    with gzip.GzipFile(fileobj=buffer, mode="wb", compresslevel=6, mtime=0) as gz:
        for record in records:
            gz.write(record.to_json_line().encode("utf-8"))
            gz.write(b"\n")
            count += 1
    return EncodedBatch(data=buffer.getvalue(), record_count=count)
