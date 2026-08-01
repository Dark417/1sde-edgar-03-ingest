"""L4: the CLI entrypoint.

Argument parsing, sink assembly, and error-to-exit-code mapping. No fetching,
no parsing, no path construction — if logic accumulates here, it belongs in a
stream handler (AGENTS.md §4).

Exit codes are contract (AGENTS.md §5.10):
    0  success, **including** a failed landing push
    1  source fetch failure
    2  config error
    3  sink write failure to S3
"""

from __future__ import annotations

import json
import sys
from datetime import date, datetime
from typing import Annotated, Any

import typer

from ingest.config import ConfigError, Settings, load_settings
from ingest.edgar.client import EdgarClient
from ingest.edgar.errors import EdgarError, IndexFormatChanged
from ingest.logging import configure_logging, get_logger
from ingest.sinks.base import Sink
from ingest.sinks.local import LocalSink
from ingest.sinks.noop import NoopSink
from ingest.sinks.s3 import S3Sink
from ingest.sinks.volume import VolumeSink
from ingest.streams import company_concept, company_submissions, filing_index
from ingest.streams.base import StreamSummary

__all__ = ["app", "main"]

EXIT_OK = 0
EXIT_FETCH_FAILED = 1
EXIT_CONFIG_ERROR = 2
EXIT_SINK_FAILED = 3

STREAMS = ("filing_index", "company_submissions", "company_concept")

app = typer.Typer(add_completion=False, help="EDGAR -> landing zone batch ingest.")
log = get_logger(__name__)


@app.command("config-check")
def config_check(
    local_only: Annotated[
        bool, typer.Option("--local-only", help="Write to a local directory only; skip AWS.")
    ] = False,
) -> None:
    """Resolve all config, print it with secrets redacted, exit 0 or 2.

    This is what you run first inside a new ECS task to find out why it will
    fail, before spending a real run discovering it.
    """
    configure_logging()
    try:
        settings = load_settings({"local_only": local_only or None})
    except ConfigError as exc:
        typer.echo(f"config error: {exc}", err=True)
        raise typer.Exit(EXIT_CONFIG_ERROR) from exc

    typer.echo(json.dumps(settings.redacted(), indent=2, default=str))
    raise typer.Exit(EXIT_OK)


@app.command("run")
def run(
    stream: Annotated[str, typer.Option("--stream", help=f"One of: {', '.join(STREAMS)}")],
    logical_date: Annotated[str, typer.Option("--logical-date", help="Logical date, YYYY-MM-DD.")],
    dry_run: Annotated[
        bool, typer.Option("--dry-run", help="Fetch and print a manifest; write nothing.")
    ] = False,
    cik_limit: Annotated[
        int | None, typer.Option("--cik-limit", help="Only the first N CIKs (cheap testing).")
    ] = None,
    resume: Annotated[
        bool, typer.Option("--resume", help="Skip (cik, concept) pairs already checkpointed.")
    ] = False,
    local_only: Annotated[
        bool, typer.Option("--local-only", help="Write to a local directory only; skip AWS.")
    ] = False,
    local_dir: Annotated[
        str | None, typer.Option("--local-dir", help="Local landing root (implies --local-only).")
    ] = None,
    log_level: Annotated[str, typer.Option("--log-level")] = "INFO",
) -> None:
    """Fetch one stream for one logical date and write it to the landing zone."""
    configure_logging(log_level)

    if stream not in STREAMS:
        typer.echo(
            f"unknown --stream {stream!r}. Valid values: {', '.join(STREAMS)}",
            err=True,
        )
        raise typer.Exit(EXIT_CONFIG_ERROR)

    try:
        parsed_date = date.fromisoformat(logical_date)
    except ValueError as exc:
        typer.echo(f"--logical-date must be YYYY-MM-DD; got {logical_date!r}", err=True)
        raise typer.Exit(EXIT_CONFIG_ERROR) from exc

    try:
        settings = load_settings(
            {
                "local_only": True if (local_only or local_dir) else None,
                "local_landing_dir": local_dir,
            }
        )
    except ConfigError as exc:
        typer.echo(f"config error: {exc}", err=True)
        raise typer.Exit(EXIT_CONFIG_ERROR) from exc

    exit_code = _execute(settings, stream, parsed_date, dry_run, cik_limit, resume)
    raise typer.Exit(exit_code)


def build_sinks(settings: Settings, dry_run: bool) -> list[Sink]:
    """Return the sinks for this run, system of record first.

    Order is the contract that ``write_to_sinks`` relies on: index 0 commits
    first and its failure is fatal; everything after it is a transport.

    ``--dry-run`` yields exactly one ``NoopSink``, so "writes nothing" is a
    property of the sink list rather than a branch inside each stream.
    """
    if dry_run:
        return [NoopSink(mode=settings.landing_mode, raw_bucket=settings.raw_bucket or "dry-run")]

    if settings.local_only:
        return [LocalSink(settings.local_landing_dir)]

    sinks: list[Sink] = [S3Sink(settings.raw_bucket or "")]
    if settings.landing_mode == "volume" and settings.dbx_host and settings.dbx_token:
        sinks.append(
            VolumeSink(
                host=settings.dbx_host,
                token=settings.dbx_token.get_secret_value(),
                volume_path=settings.volume_path,
            )
        )
    return sinks


def _execute(
    settings: Settings,
    stream: str,
    logical_date: date,
    dry_run: bool,
    cik_limit: int | None,
    resume: bool,
) -> int:
    """Run one stream and map its outcome to an exit code."""
    started = datetime.now().timestamp()
    sinks = build_sinks(settings, dry_run)
    client = EdgarClient(user_agent=settings.sec_user_agent, max_rps=settings.max_rps)

    try:
        with client:
            if stream == "filing_index":
                summary = filing_index.run(client, sinks, logical_date)
            elif stream == "company_submissions":
                summary = company_submissions.run(
                    client, sinks, logical_date, settings.cik_universe_uri, cik_limit
                )
            else:
                summary = company_concept.run(
                    client,
                    sinks,
                    logical_date,
                    settings.cik_universe_uri,
                    cik_limit,
                    resume=resume,
                )
    except IndexFormatChanged as exc:
        log.error("INDEX_FORMAT_CHANGED", stream=stream, error=str(exc))
        return EXIT_FETCH_FAILED
    except EdgarError as exc:
        log.error("SOURCE_FETCH_FAILED", stream=stream, error=str(exc))
        return EXIT_FETCH_FAILED
    except Exception as exc:
        log.error("SINK_WRITE_FAILED", stream=stream, error=str(exc))
        return EXIT_SINK_FAILED

    duration_s = round(datetime.now().timestamp() - started, 3)
    if dry_run:
        _print_manifest(settings, summary, logical_date)

    log.info("ingest_complete", duration_s=duration_s, dry_run=dry_run, **summary.as_log_fields())
    return EXIT_OK


def _print_manifest(settings: Settings, summary: StreamSummary, logical_date: date) -> None:
    """Print what a real run would have written.

    Both target paths are printed — same filename, different prefix — because
    seeing them differ is how you catch a path bug before it reaches S3.
    """
    from edgar_lakehouse_contracts.names import landing_path

    targets = {
        "s3": landing_path(
            "s3", summary.stream, logical_date, raw_bucket=settings.raw_bucket or "<raw_bucket>"
        ),
        "volume": landing_path("volume", summary.stream, logical_date),
    }
    if settings.local_only:
        targets["local"] = str(
            LocalSink(settings.local_landing_dir).target_path(summary.stream, logical_date)
        )

    manifest: dict[str, Any] = {
        "dry_run": True,
        "stream": str(summary.stream.value),
        "logical_date": logical_date.isoformat(),
        "batch_id": summary.batch_id,
        "records": summary.records,
        "bytes": summary.bytes_written,
        "requests": summary.requests,
        "targets": targets,
    }
    typer.echo(json.dumps(manifest, indent=2))


def main() -> None:
    """Console-script entrypoint."""
    app()


if __name__ == "__main__":
    sys.exit(app())  # pragma: no cover
