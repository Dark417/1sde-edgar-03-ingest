"""L3: the ``company_concept`` stream — universe x CONCEPT_SET, with resume.

This is the expensive stream: 15 concepts x 500 companies = 7,500 requests, or
~25 minutes at 5 rps. A crash at request 6,000 must not restart at zero, so
completed ``(cik, concept)`` pairs are checkpointed to a local NDJSON file as
they finish and ``--resume`` skips them (AGENTS.md §6 F-4).

Because the checkpoint stores the **envelopes themselves**, a resumed run lands
the same object an uninterrupted one would. ``_fetched_at`` differs per record,
but that field is metadata and is explicitly excluded from keys and filenames
(data contracts §1).

Does not handle: parallelism. The rate limit, not the request count, is the
constraint — concurrency would only produce 429s faster.
"""

from __future__ import annotations

import json
import tempfile
from collections.abc import Sequence
from datetime import date
from pathlib import Path
from typing import Any

from edgar_lakehouse_contracts.concepts import CONCEPT_SET
from edgar_lakehouse_contracts.envelope import LandingEnvelope
from edgar_lakehouse_contracts.names import Stream, batch_id

from ingest.edgar.client import EdgarClient
from ingest.logging import get_logger
from ingest.sinks.base import Sink
from ingest.streams.base import StreamSummary, build_envelope, load_cik_universe, write_to_sinks

__all__ = ["DEFAULT_CHECKPOINT_DIR", "TAXONOMY", "checkpoint_path", "run"]

log = get_logger(__name__)

TAXONOMY = "us-gaap"
DEFAULT_CHECKPOINT_DIR = Path(tempfile.gettempdir()) / "ingest-checkpoints"


def checkpoint_path(logical_date: date, checkpoint_dir: Path | None = None) -> Path:
    """Return the checkpoint file path for a batch.

    Keyed by ``batch_id``, so a checkpoint can never be applied to a different
    (stream, logical_date) than the one that produced it.
    """
    directory = checkpoint_dir or DEFAULT_CHECKPOINT_DIR
    return directory / f"{batch_id(Stream.COMPANY_CONCEPT, logical_date)}.ndjson"


def run(
    client: EdgarClient,
    sinks: Sequence[Sink],
    logical_date: date,
    universe_uri: str | None = None,
    cik_limit: int | None = None,
    resume: bool = False,
    checkpoint_dir: Path | None = None,
) -> StreamSummary:
    """Fetch every (CIK, concept) pair and land them as one batch.

    A 404 means the company does not report that concept, which is normal and
    not an error (design doc §4.2.5) — Apple does not report
    ``us-gaap:CostOfRevenue``, for example. The pair is still checkpointed as
    *done* so a resumed run does not re-request a URL already known to 404.
    """
    summary = StreamSummary(
        stream=Stream.COMPANY_CONCEPT,
        logical_date=logical_date,
        batch_id=batch_id(Stream.COMPANY_CONCEPT, logical_date),
    )
    ciks = load_cik_universe(universe_uri, limit=cik_limit)
    path = checkpoint_path(logical_date, checkpoint_dir)

    completed: dict[tuple[str, str], LandingEnvelope | None] = {}
    if resume:
        completed = _read_checkpoint(path)
        log.info(
            "resume_from_checkpoint",
            path=str(path),
            completed_pairs=len(completed),
            note="already-done (cik, concept) pairs will not be re-requested",
        )
    elif path.exists():
        # A stale checkpoint from a previous crash must not leak into a fresh
        # run: without --resume the operator asked for a clean pass.
        path.unlink()

    path.parent.mkdir(parents=True, exist_ok=True)
    log.info(
        "concept_fanout",
        ciks=len(ciks),
        concepts=len(CONCEPT_SET),
        planned_requests=len(ciks) * len(CONCEPT_SET) - len(completed),
    )

    with path.open("a", encoding="utf-8") as checkpoint:
        for cik in ciks:
            for concept in CONCEPT_SET:
                if (cik, concept) in completed:
                    continue
                summary.requests += 1
                payload = client.fetch_company_concept(cik, TAXONOMY, concept)
                envelope = (
                    build_envelope(
                        stream=Stream.COMPANY_CONCEPT,
                        logical_date=logical_date,
                        source_url=client.company_concept_url(cik, TAXONOMY, concept),
                        payload=payload,
                    )
                    if payload is not None
                    else None
                )
                completed[(cik, concept)] = envelope
                _append_checkpoint(checkpoint, cik, concept, envelope)

    # Sorted so a resumed run and an uninterrupted run produce the same object.
    envelopes = [
        envelope for _, envelope in sorted(completed.items(), key=lambda kv: kv[0]) if envelope
    ]
    write_to_sinks(sinks, Stream.COMPANY_CONCEPT, logical_date, envelopes, summary)

    path.unlink(missing_ok=True)
    return summary


def _append_checkpoint(
    handle: Any, cik: str, concept: str, envelope: LandingEnvelope | None
) -> None:
    """Record one completed pair and flush it.

    Flushed per record on purpose: an unflushed checkpoint is not a checkpoint,
    and the write is cheap next to a rate-limited HTTP request.
    """
    line = {
        "cik": cik,
        "concept": concept,
        "envelope": json.loads(envelope.to_json_line()) if envelope else None,
    }
    handle.write(json.dumps(line) + "\n")
    handle.flush()


def _read_checkpoint(path: Path) -> dict[tuple[str, str], LandingEnvelope | None]:
    """Load completed pairs from a checkpoint file.

    A truncated final line (the normal shape of a crash mid-write) is skipped
    rather than fatal: the pair is simply re-requested.
    """
    if not path.exists():
        return {}

    completed: dict[tuple[str, str], LandingEnvelope | None] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        if not raw.strip():
            continue
        try:
            record = json.loads(raw)
        except json.JSONDecodeError:
            log.warning("checkpoint_line_truncated", path=str(path), note="pair will be refetched")
            continue
        envelope_data = record.get("envelope")
        envelope = LandingEnvelope.model_validate(envelope_data) if envelope_data else None
        completed[(record["cik"], record["concept"])] = envelope
    return completed
