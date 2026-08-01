"""L3: the ``filing_index`` stream — one daily index file per logical date.

Does not handle: parsing (edgar.parsers) or path construction (contracts).
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date

from edgar_lakehouse_contracts.names import Stream, batch_id

from ingest.edgar.client import EdgarClient
from ingest.edgar.errors import NoIndexForDate
from ingest.logging import get_logger
from ingest.sinks.base import Sink
from ingest.streams.base import StreamSummary, build_envelope, write_to_sinks

__all__ = ["run"]

log = get_logger(__name__)


def run(
    client: EdgarClient,
    sinks: Sequence[Sink],
    logical_date: date,
) -> StreamSummary:
    """Fetch one daily form index and land it as one batch.

    A weekend or market holiday raises ``NoIndexForDate`` inside the client;
    that is caught here and reported as a **successful run with zero records**.
    A Saturday having no filings is not a failure, and treating it as one would
    page somebody every weekend (design doc §4.2.4).
    """
    summary = StreamSummary(
        stream=Stream.FILING_INDEX,
        logical_date=logical_date,
        batch_id=batch_id(Stream.FILING_INDEX, logical_date),
    )
    source_url = client.daily_index_url(logical_date)

    try:
        records = list(client.fetch_daily_index(logical_date))
    except NoIndexForDate as exc:
        log.info(
            "no_index_for_date",
            stream=str(Stream.FILING_INDEX.value),
            logical_date=logical_date.isoformat(),
            reason=str(exc),
            note="weekend or market holiday: zero filings is not a failure",
        )
        summary.requests = 1
        return summary

    summary.requests = 1
    envelopes = [
        build_envelope(
            stream=Stream.FILING_INDEX,
            logical_date=logical_date,
            source_url=source_url,
            payload=record.model_dump(),
        )
        for record in records
    ]
    write_to_sinks(sinks, Stream.FILING_INDEX, logical_date, envelopes, summary)
    return summary
