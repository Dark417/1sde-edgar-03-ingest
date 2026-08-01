"""L2: the Databricks Volume sink — a transport that is allowed to fail.

Uses the Files API directly over httpx. The ``databricks-sdk`` is a forbidden
dependency here (AGENTS.md §3): the upload is three lines of httpx, and the SDK
would pull a large dependency tree into a container that runs for ninety
seconds.

Does not handle: volume creation (repo 2), or deciding that its own failure is
non-fatal — the stream handler does that.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import date

import httpx
from edgar_lakehouse_contracts.envelope import LandingEnvelope
from edgar_lakehouse_contracts.names import Stream, landing_path

from ingest.logging import get_logger
from ingest.sinks.base import SinkResult, encode_batch

__all__ = ["VolumeSink"]

log = get_logger(__name__)


class VolumeSink:
    """Uploads a batch to a Databricks Volume via the Files API.

    The target path is ``names.landing_path("volume", ...)`` — the same filename
    the S3 sink uses, only a different prefix. That is what lets a replay from
    S3 reproduce the live path exactly (ADR-001).
    """

    def __init__(
        self,
        host: str,
        token: str,
        volume_path: str = "/Volumes/edgar/landing/edgar",
        client: httpx.Client | None = None,
        timeout: float = 120.0,
    ) -> None:
        if not host or not token:
            raise ValueError("VolumeSink requires both a host and a token")
        self.host = host.rstrip("/")
        self.volume_path = volume_path.rstrip("/")
        self._token = token
        self._client = client or httpx.Client(timeout=httpx.Timeout(timeout))

    def close(self) -> None:
        """Close the underlying HTTP client."""
        self._client.close()

    def write(
        self, stream: Stream, logical_date: date, records: Iterable[LandingEnvelope]
    ) -> SinkResult:
        """PUT the batch to the Volume, overwriting any existing object.

        The body is streamed from a reader rather than passed as one ``bytes``
        object, so a large concept batch is not copied again on its way out.

        Raises on any non-2xx. The caller catches that and continues — a failed
        landing push must never fail the run.
        """
        batch = encode_batch(records)
        target = landing_path("volume", stream, logical_date)
        # landing_path returns the canonical /Volumes/... path; honour a
        # configured volume_path override without recomputing the layout.
        relative = target.removeprefix("/Volumes/edgar/landing/edgar").lstrip("/")
        destination = f"{self.volume_path}/{relative}"
        url = f"{self.host}/api/2.0/fs/files{destination}"

        response = self._client.put(
            url,
            params={"overwrite": "true"},
            headers={
                "Authorization": f"Bearer {self._token}",
                "Content-Type": "application/octet-stream",
            },
            content=batch.reader(),
        )
        response.raise_for_status()

        log.info(
            "sink_write",
            sink="volume",
            uri=destination,
            bytes=len(batch.data),
            records=batch.record_count,
        )
        return SinkResult(
            uri=destination, bytes_written=len(batch.data), record_count=batch.record_count
        )
