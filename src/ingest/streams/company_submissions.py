"""L3: the ``company_submissions`` stream — one document per CIK in the universe.

Does not handle: flattening the submissions document. It is deeply nested and
its shape is not ours to control; bronze stores it as one JSON string
(data contracts §2.2).
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date

from edgar_lakehouse_contracts.names import Stream, batch_id

from ingest.edgar.client import EdgarClient
from ingest.edgar.errors import FetchFailed
from ingest.logging import get_logger
from ingest.sinks.base import Sink
from ingest.streams.base import StreamSummary, build_envelope, load_cik_universe, write_to_sinks

__all__ = ["run"]

log = get_logger(__name__)


def run(
    client: EdgarClient,
    sinks: Sequence[Sink],
    logical_date: date,
    universe_uri: str | None = None,
    cik_limit: int | None = None,
) -> StreamSummary:
    """Fetch the submissions document for each CIK and land them as one batch.

    A CIK whose document cannot be fetched is logged and skipped rather than
    failing the whole run — one bad CIK in a 500-company universe should not
    cost the other 499. A universe that is entirely unfetchable still surfaces,
    because the batch then lands zero records.
    """
    summary = StreamSummary(
        stream=Stream.COMPANY_SUBMISSIONS,
        logical_date=logical_date,
        batch_id=batch_id(Stream.COMPANY_SUBMISSIONS, logical_date),
    )
    ciks = load_cik_universe(universe_uri, limit=cik_limit)
    log.info("universe_loaded", stream=str(Stream.COMPANY_SUBMISSIONS.value), ciks=len(ciks))

    envelopes = []
    for cik in ciks:
        summary.requests += 1
        try:
            payload = client.fetch_submissions(cik)
        except FetchFailed as exc:
            log.warning("submissions_fetch_failed", cik=cik, error=str(exc))
            continue
        envelopes.append(
            build_envelope(
                stream=Stream.COMPANY_SUBMISSIONS,
                logical_date=logical_date,
                source_url=client.submissions_url(cik),
                payload=payload,
            )
        )

    write_to_sinks(sinks, Stream.COMPANY_SUBMISSIONS, logical_date, envelopes, summary)
    return summary
