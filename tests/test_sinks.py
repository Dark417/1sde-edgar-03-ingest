"""The sinks (F-3 acceptance) — byte-identity and idempotency.

The byte-identity test is the one that matters most: if the sinks diverge, an
S3 replay produces different bronze than the live path did, and you do not find
out until you need the replay.
"""

from __future__ import annotations

import gzip
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import boto3
import httpx
import pytest
from edgar_lakehouse_contracts.names import Stream, landing_path
from moto import mock_aws
from tests.conftest import LOGICAL_DATE

from ingest.sinks.base import encode_batch
from ingest.sinks.local import LocalSink
from ingest.sinks.noop import NoopSink
from ingest.sinks.s3 import S3Sink
from ingest.sinks.volume import VolumeSink

BUCKET = "edgar-lake-raw-test"


@pytest.fixture
def s3_client() -> Any:
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield client


def capture_volume_put() -> tuple[httpx.Client, dict[str, Any]]:
    """Return an httpx client that records the PUT body and URL."""
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = request.read()
        captured["url"] = str(request.url)
        captured["headers"] = dict(request.headers)
        return httpx.Response(200)

    return httpx.Client(transport=httpx.MockTransport(handler)), captured


# ------------------------------------------------------------ byte identity


def test_all_three_sinks_write_identical_bytes(
    s3_client: Any, tmp_path: Path, make_envelopes: Callable[[int], list[Any]]
) -> None:
    """THE test (AGENTS.md §7). Divergence here is silent until replay day."""
    envelopes = make_envelopes(50)

    S3Sink(BUCKET, client=s3_client).write(Stream.FILING_INDEX, LOGICAL_DATE, envelopes)
    s3_bytes = s3_client.get_object(
        Bucket=BUCKET,
        Key=f"edgar/filing_index/dt={LOGICAL_DATE.isoformat()}/"
        f"{landing_path('s3', Stream.FILING_INDEX, LOGICAL_DATE).rsplit('/', 1)[1]}",
    )["Body"].read()

    volume_client, captured = capture_volume_put()
    VolumeSink("https://dbx.example.com", "tok", client=volume_client).write(
        Stream.FILING_INDEX, LOGICAL_DATE, envelopes
    )
    volume_bytes = captured["body"]

    local_result = LocalSink(tmp_path).write(Stream.FILING_INDEX, LOGICAL_DATE, envelopes)
    local_bytes = Path(local_result.uri).read_bytes()

    assert s3_bytes == volume_bytes == local_bytes


def test_encoder_is_deterministic(make_envelopes: Callable[[int], list[Any]]) -> None:
    """gzip mtime=0: the default embeds wall clock and would break identity."""
    envelopes = make_envelopes(20)
    assert encode_batch(envelopes).data == encode_batch(envelopes).data


def test_gzip_header_carries_no_timestamp(make_envelopes: Callable[[int], list[Any]]) -> None:
    data = encode_batch(make_envelopes(3)).data
    assert data[4:8] == b"\x00\x00\x00\x00"  # MTIME field of the gzip header


# ---------------------------------------------------------------- encoding


def test_output_is_gzip_ndjson_one_envelope_per_line(
    make_envelopes: Callable[[int], list[Any]],
) -> None:
    batch = encode_batch(make_envelopes(5))
    lines = gzip.decompress(batch.data).decode().splitlines()
    assert len(lines) == 5
    for line in lines:
        record = json.loads(line)
        assert set(record) == {
            "_stream",
            "_logical_date",
            "_batch_id",
            "_fetched_at",
            "_source_url",
            "_schema_version",
            "payload",
        }
        assert record["_logical_date"] == "2026-07-29"  # a date, never a datetime


def test_empty_batch_produces_a_valid_empty_file() -> None:
    batch = encode_batch([])
    assert batch.record_count == 0
    assert gzip.decompress(batch.data) == b""


# ------------------------------------------------------------- idempotency


def test_rerun_writes_the_same_key_and_leaves_one_object(
    s3_client: Any, make_envelopes: Callable[[int], list[Any]]
) -> None:
    """Run it twice -> one object, same key (§8.1). Two means bronze doubles."""
    sink = S3Sink(BUCKET, client=s3_client)
    first = sink.write(Stream.FILING_INDEX, LOGICAL_DATE, make_envelopes(10))
    second = sink.write(Stream.FILING_INDEX, LOGICAL_DATE, make_envelopes(10))

    assert first.uri == second.uri
    listing = s3_client.list_objects_v2(Bucket=BUCKET)
    assert listing["KeyCount"] == 1


def test_local_sink_rerun_leaves_one_file(
    tmp_path: Path, make_envelopes: Callable[[int], list[Any]]
) -> None:
    sink = LocalSink(tmp_path)
    sink.write(Stream.FILING_INDEX, LOGICAL_DATE, make_envelopes(4))
    sink.write(Stream.FILING_INDEX, LOGICAL_DATE, make_envelopes(4))
    directory = tmp_path / "edgar" / "filing_index" / f"dt={LOGICAL_DATE.isoformat()}"
    assert len(list(directory.iterdir())) == 1


def test_filename_does_not_depend_on_wall_clock(
    make_envelopes: Callable[[int], list[Any]], tmp_path: Path
) -> None:
    """§5.7: a timestamped filename makes a re-run look like new data."""
    from freezegun import freeze_time

    sink = LocalSink(tmp_path)
    with freeze_time("2026-08-01 06:00:00"):
        first = sink.write(Stream.FILING_INDEX, LOGICAL_DATE, make_envelopes(2))
    with freeze_time("2027-01-15 23:59:59"):
        second = sink.write(Stream.FILING_INDEX, LOGICAL_DATE, make_envelopes(2))
    assert first.uri == second.uri


# -------------------------------------------------------------------- s3


def test_s3_sets_gzip_content_encoding(
    s3_client: Any, make_envelopes: Callable[[int], list[Any]]
) -> None:
    S3Sink(BUCKET, client=s3_client).write(Stream.FILING_INDEX, LOGICAL_DATE, make_envelopes(2))
    key = s3_client.list_objects_v2(Bucket=BUCKET)["Contents"][0]["Key"]
    assert s3_client.head_object(Bucket=BUCKET, Key=key)["ContentEncoding"] == "gzip"


def test_s3_key_matches_the_contract_path(
    s3_client: Any, make_envelopes: Callable[[int], list[Any]]
) -> None:
    result = S3Sink(BUCKET, client=s3_client).write(
        Stream.FILING_INDEX, LOGICAL_DATE, make_envelopes(1)
    )
    assert result.uri == landing_path("s3", Stream.FILING_INDEX, LOGICAL_DATE, raw_bucket=BUCKET)


def test_s3_sink_requires_a_bucket() -> None:
    with pytest.raises(ValueError, match="raw_bucket"):
        S3Sink("")


# ---------------------------------------------------------------- volume


def test_volume_uses_files_api_with_overwrite(
    make_envelopes: Callable[[int], list[Any]],
) -> None:
    client, captured = capture_volume_put()
    VolumeSink("https://dbx.example.com/", "tok-123", client=client).write(
        Stream.FILING_INDEX, LOGICAL_DATE, make_envelopes(2)
    )
    assert captured["url"].startswith(
        "https://dbx.example.com/api/2.0/fs/files/Volumes/edgar/landing/edgar/filing_index/"
    )
    assert "overwrite=true" in captured["url"]
    assert captured["headers"]["authorization"] == "Bearer tok-123"


def test_volume_and_s3_share_a_filename(
    s3_client: Any, make_envelopes: Callable[[int], list[Any]]
) -> None:
    """Same filename, different prefix — what makes replay reproduce the live path."""
    client, _ = capture_volume_put()
    volume = VolumeSink("https://dbx.example.com", "tok", client=client).write(
        Stream.FILING_INDEX, LOGICAL_DATE, make_envelopes(1)
    )
    s3 = S3Sink(BUCKET, client=s3_client).write(
        Stream.FILING_INDEX, LOGICAL_DATE, make_envelopes(1)
    )
    assert volume.uri.rsplit("/", 1)[1] == s3.uri.rsplit("/", 1)[1]
    assert volume.uri != s3.uri


def test_volume_raises_on_non_2xx(make_envelopes: Callable[[int], list[Any]]) -> None:
    client = httpx.Client(transport=httpx.MockTransport(lambda r: httpx.Response(500)))
    with pytest.raises(httpx.HTTPStatusError):
        VolumeSink("https://dbx.example.com", "tok", client=client).write(
            Stream.FILING_INDEX, LOGICAL_DATE, make_envelopes(1)
        )


def test_volume_requires_host_and_token() -> None:
    with pytest.raises(ValueError, match="host and a token"):
        VolumeSink("", "tok")
    with pytest.raises(ValueError, match="host and a token"):
        VolumeSink("https://dbx", "")


def test_volume_honours_a_custom_volume_path(make_envelopes: Callable[[int], list[Any]]) -> None:
    client, _ = capture_volume_put()
    result = VolumeSink(
        "https://dbx.example.com", "tok", volume_path="/Volumes/other/zone", client=client
    ).write(Stream.FILING_INDEX, LOGICAL_DATE, make_envelopes(1))
    assert result.uri.startswith("/Volumes/other/zone/filing_index/")


# ----------------------------------------------------------------- local


def test_local_layout_mirrors_the_volume_layout(
    tmp_path: Path, make_envelopes: Callable[[int], list[Any]]
) -> None:
    """So promoting the local tree to a Volume is a copy, not a rewrite."""
    result = LocalSink(tmp_path).write(Stream.FILING_INDEX, LOGICAL_DATE, make_envelopes(1))
    canonical = landing_path("volume", Stream.FILING_INDEX, LOGICAL_DATE)
    relative = canonical.removeprefix("/Volumes/edgar/landing/edgar").lstrip("/")
    assert Path(result.uri) == tmp_path / "edgar" / relative


def test_local_sink_requires_a_root() -> None:
    with pytest.raises(ValueError, match="root"):
        LocalSink("")


# ------------------------------------------------------------------ noop


def test_noop_writes_nothing_but_reports_a_target(
    tmp_path: Path, make_envelopes: Callable[[int], list[Any]]
) -> None:
    result = NoopSink(mode="s3", raw_bucket=BUCKET).write(
        Stream.FILING_INDEX, LOGICAL_DATE, make_envelopes(7)
    )
    assert result.record_count == 7
    assert result.bytes_written > 0  # it still encodes, so the count is truthful
    assert result.uri.startswith(f"s3://{BUCKET}/")
    assert not list(tmp_path.iterdir())
