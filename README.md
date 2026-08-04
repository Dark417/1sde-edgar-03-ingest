# 1sde-edgar-03-ingest

> **Part of the [EDGAR lakehouse](https://github.com/Dark417/1sde-edgar-06-chatbot#readme)
> project.** That README is the front door: the dataflow, the Databricks layers, how the
> chatbot answers, how the six repositories fit together, and what it costs — one diagram
> each.
>
> **Live:** [the site](https://edgar.xiaoxiaolei.com) ·
> [SEC EDGAR](https://www.sec.gov/edgar), the source of every figure.


Repo 3 of 6. Pulls filings from SEC EDGAR once a day and writes them exactly as
received to S3 and to a Databricks volume. Nothing is parsed here, so any later mistake
can be re-derived without asking the SEC again.

Repo 3 of 5 in the **edgar lakehouse**: a batch CLI that fetches from SEC EDGAR
and writes raw records to the landing zone. It is the only component in the
project that touches the public internet.

It owns no schema (those are imported from `edgar_lakehouse_contracts`), has no
Spark dependency, and performs no transformation — payloads pass through
verbatim.

> **The one design idea that governs this repo:** S3 is the system of record and
> commits first. The Databricks landing push is a *transport* that is allowed to
> fail. Ingest is never blocked by Databricks being down.

## Quick start (local)

The production path (EDGAR → S3 → Databricks) needs repo 2's AWS
infrastructure — applied, but the schedule is disabled and the remote path has
not had its first validated run. Until then the working mode is local: pull a
small subset to disk and load it into tables by hand. See
[`docs/04-local-workflow.md`](docs/04-local-workflow.md).

```bash
uv venv --python 3.11 && source .venv/bin/activate

# repo 1's contracts wheel — from its GitHub release (not on PyPI); the tag
# must match the pin in pyproject.toml
gh release download v1.1.0 --repo Dark417/1sde-edgar-01-contracts \
  --pattern 'edgar_lakehouse_contracts-*.whl' --dir wheels
uv pip install --find-links wheels -e ".[dev]"

# The only thing that must be set. No AWS, no SSM, no bucket.
export SEC_USER_AGENT="edgar-lakehouse-demo you@example.com"   # must contain an @

python -m ingest.cli config-check
python -m ingest.cli run --stream filing_index --logical-date 2026-07-29
```

**Local is the default.** It is the only path validated end to end so far, and
the path that works should be the one that needs no configuration. The S3 +
Volume path is opt-in via `--remote` (or `LOCAL_ONLY=false`), and asking for it
without a `RAW_BUCKET` is a hard exit 2 — it never quietly downgrades to a
local write.

Output is gzip NDJSON, one landing envelope per line:

```
local-landing/edgar/filing_index/logical_date=2026-07-29/filing_index-20260729-eb4807cfccc9.json.gz
```

## Commands

```
python -m ingest.cli run --stream {filing_index|company_submissions|company_concept}
                         --logical-date YYYY-MM-DD
                         [--dry-run] [--cik-limit N] [--resume]
                         [--local-only | --remote] [--local-dir PATH]
python -m ingest.cli config-check
```

`--dry-run` fetches, prints a manifest (record count, target paths), and writes
nothing. `config-check` resolves all config, prints it with secrets redacted,
and exits 0 or 2 — run it first inside a new ECS task to find out why it will
fail.

## Streams

| Stream | Source | Volume |
|---|---|---|
| `filing_index` | daily form index `.idx` (fixed-width) | 1 request, ~6k records/day |
| `company_submissions` | `data.sec.gov/submissions/CIK{cik10}.json` | 1 request per CIK |
| `company_concept` | `data.sec.gov/api/xbrl/companyconcept/...` | universe × 15 concepts, resumable |

## Exit codes are contract

| Code | Meaning |
|---|---|
| 0 | success — **including** a failed landing push |
| 1 | source fetch failure |
| 2 | config error |
| 3 | S3 sink write failure |

## Development

```bash
ruff check src tests && ruff format --check src tests
mypy                      # --strict
pytest                    # coverage gate >= 85%; zero network calls
```

Tests never touch the network: every request is served by `respx` from real
EDGAR samples committed under `tests/fixtures/` (see the provenance table in
[`tests/fixtures/README.md`](tests/fixtures/README.md)).

## Docs

| Doc | What it is |
|---|---|
| [`docs/00-design-doc.md`](docs/00-design-doc.md) | authoritative, copied from repo 1 |
| [`docs/02-data-contracts.md`](docs/02-data-contracts.md) | authoritative, copied from repo 1 |
| [`docs/ADR-001-landing-transport.md`](docs/ADR-001-landing-transport.md) | which sink Auto Loader reads from |
| [`docs/03-ingest-design.md`](docs/03-ingest-design.md) | this repo's design: layers, parser, sinks, resume |
| [`docs/04-local-workflow.md`](docs/04-local-workflow.md) | pull a subset locally, load into tables by hand |

`AGENTS.md` carries the full specification and the manual execution runbook.
