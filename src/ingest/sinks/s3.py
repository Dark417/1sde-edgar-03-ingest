"""L2: the S3 sink — the system of record.

S3 commits **first** and its failure is fatal (exit code 3). Everything else in
the landing story is a transport that may fail (design doc §5.1).

Does not handle: bucket creation or lifecycle — repo 2's Terraform owns those.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import date
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

from edgar_lakehouse_contracts.envelope import LandingEnvelope
from edgar_lakehouse_contracts.names import Stream, landing_path

from ingest.logging import get_logger
from ingest.sinks.base import SinkResult, encode_batch

if TYPE_CHECKING:  # pragma: no cover - typing only
    from mypy_boto3_s3.client import S3Client

__all__ = ["S3Sink"]

log = get_logger(__name__)


class S3Sink:
    """Writes a batch as one gzip NDJSON object to the raw bucket.

    The key comes from ``names.landing_path`` and is deterministic in
    ``(stream, logical_date)``, so re-running overwrites rather than duplicating
    (design doc §8.1). Overwriting is the intended behaviour, not a hazard: the
    contents are deterministic too.
    """

    def __init__(self, raw_bucket: str, client: Any | None = None) -> None:
        if not raw_bucket:
            raise ValueError("raw_bucket is required for S3Sink")
        self.raw_bucket = raw_bucket
        self._client: S3Client | None = client

    @property
    def client(self) -> S3Client:
        """The boto3 S3 client, created lazily.

        Lazily so that constructing the sink — which the CLI does before it
        knows whether ``--dry-run`` is set — never requires AWS credentials.
        """
        if self._client is None:
            import boto3

            self._client = boto3.client("s3")
        return self._client

    def write(
        self, stream: Stream, logical_date: date, records: Iterable[LandingEnvelope]
    ) -> SinkResult:
        """Put the batch and return what was written.

        Does not handle: retrying. boto3's standard retry mode already covers
        the transient cases, and an S3 failure is meant to be fatal.
        """
        batch = encode_batch(records)
        uri = landing_path("s3", stream, logical_date, raw_bucket=self.raw_bucket)
        key = urlparse(uri).path.lstrip("/")

        self.client.put_object(
            Bucket=self.raw_bucket,
            Key=key,
            Body=batch.data,
            ContentType="application/x-ndjson",
            ContentEncoding="gzip",
        )
        log.info(
            "sink_write",
            sink="s3",
            uri=uri,
            bytes=len(batch.data),
            records=batch.record_count,
        )
        return SinkResult(uri=uri, bytes_written=len(batch.data), record_count=batch.record_count)
