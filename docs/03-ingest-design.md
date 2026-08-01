# Ingest design — repo 3

> Derived from `00-design-doc.md` and `02-data-contracts.md`, which are authoritative
> and copied verbatim from repo 1 (`edgar-lakehouse-contracts` @ `07a9808`, v0.1.0).
> Where this document and those disagree, those win and this document is a bug.

## 1. What this repo is

A batch CLI that fetches from SEC EDGAR and writes raw records to the landing zone.
It is the only component in the project that touches the public internet. It has no
Spark, performs no transformation, and defines no schema.

The governing idea (design doc §5.1): **S3 is the system of record and commits first.**
The Databricks landing push is a transport that is allowed to fail. A failed Volume
`PUT` logs `LANDING_PUSH_FAILED` and exits 0. Failing the run because Databricks was
down would defeat the replay story that justifies the raw zone existing.

## 2. Layers

`L0` imports nothing internal; each layer imports only layers below it.

| Layer | Module | Responsibility |
|---|---|---|
| L0 | `config.py` | `Settings`, env → SSM resolution, validation |
| L0 | `logging.py` | structlog JSON-to-stdout setup |
| L1 | `edgar/errors.py` | typed exceptions |
| L1 | `edgar/parsers.py` | fixed-width `.idx` parser |
| L1 | `edgar/client.py` | HTTP, token-bucket rate limit, retry |
| L1 | `sinks/base.py` | `Sink` protocol, `SinkResult`, envelope→gzip-NDJSON encoding |
| L2 | `sinks/{s3,volume,local,noop}.py` | the four sinks |
| L3 | `streams/*.py` | fetch → envelope → sinks → summary |
| L4 | `cli.py` | argument parsing only, no logic |

`cli.py` containing logic is the failure mode this table exists to prevent.

## 3. The `.idx` parser

The daily index is **fixed-width, not CSV** (design doc §4.2.3). The layout was
measured from a real file (`tests/fixtures/form.20260729.idx`, 2026-07-29, 5,969 data
rows) rather than assumed:

| Field | Slice | Width |
|---|---|---|
| `form_type` | `[0:17]` | 17 |
| `company_name` | `[17:79]` | 62 |
| `cik` | `[79:91]` | 12 |
| `date_filed` | `[91:103]` | 12 |
| `file_name` | `[103:]` | rest |

Widths were derived by finding the character positions that are blank in **all** 5,969
data rows: gutters at 15–16, 77–78, 86–90, 99–102. Note that the *header* line claims
`Company Name` begins at column 12; the data disagrees and begins at 17. The header is
decorative — trust the data.

Why splitting on whitespace is wrong: the fixture contains the form type
`SEC STAFF ACTION`, which has a space in it, and company names contain runs of spaces.
A `split()`-based parser silently produces garbage for those rows, which is the single
worst available outcome (`AGENTS.md` §5.8).

**Header validation.** The header block is hard-wrapped by EDGAR across two lines
(`... CIK` / `      Date Filed  File Name`), so validation normalizes whitespace across
the block and asserts the token sequence
`Form Type Company Name CIK Date Filed File Name`, plus the presence of the dashed
separator line. A mismatch raises `IndexFormatChanged` and returns **zero** rows —
never a partial or best-effort parse.

Each parsed row is additionally structurally checked: `cik` must be digits and
`date_filed` must be 8 digits. A row failing that also raises `IndexFormatChanged`,
because it means the column boundaries have shifted even though the header did not.

## 4. Rate limiting and retries

- **Token bucket**, not `sleep(0.2)` — a sleep-per-request serializes at exactly the
  wrong granularity and cannot absorb a burst. Default 5 rps, hard cap 8 (design doc
  §4.2.1); constructing a client above the cap raises.
- The clock is injected so the limiter is testable without wall-clock sleeps.
- **Retry 429 and 5xx only**, exponential backoff with jitter, max 5 attempts.
- **403 is never retried.** It means the `User-Agent` is wrong; retrying makes it
  worse and is how you get banned. **404 is never retried** either.
- `User-Agent` must be present and contain an `@`; validated at client construction so
  the failure is a clear startup error rather than 403s at 06:00 UTC.

## 5. The 404s that are not errors

| Case | Behaviour |
|---|---|
| `filing_index` on a weekend/holiday | raise typed `NoIndexForDate`; the CLI treats it as success with zero records |
| `company_concept` for an unreported concept | return `None`, log at DEBUG, continue |

The second is not hypothetical: **Apple (CIK 0000320193) genuinely does not report
`us-gaap:CostOfRevenue`** — that URL 404s today. Treating it as an error would fail
every single run. Note also that the 404 body is **XML**, not JSON (see
`tests/fixtures/companyconcept_404_body.xml`), so the client must decide on status
code and never attempt to parse the body.

## 6. Sinks

```python
@dataclass(frozen=True)
class SinkResult:
    uri: str
    bytes_written: int
    record_count: int
```

All sinks receive the identical `bytes` object produced by one shared encoder in
`sinks/base.py`. This is deliberate: byte-identity between sinks is not something the
sinks each try to achieve, it is something they cannot avoid.

| Sink | Target | Failure behaviour |
|---|---|---|
| `S3Sink` | `landing_path("s3", ...)` | fatal — exit 3 |
| `VolumeSink` | Databricks Files API `PUT ...?overwrite=true` | non-fatal — `LANDING_PUSH_FAILED`, exit 0 |
| `LocalSink` | a local directory (see §8) | non-fatal, same as Volume |
| `NoopSink` | nothing | n/a — used by `--dry-run` |

Gzip is written with `mtime=0`. The default `gzip` header embeds the current time,
which would make two runs of the same batch produce different bytes and quietly break
the byte-identity property. Object *keys* are already deterministic via
`names.batch_id`; the *contents* have to be too.

## 7. Exit codes are contract

| Code | Meaning |
|---|---|
| 0 | success — **including** a failed landing push |
| 1 | source fetch failure |
| 2 | config error |
| 3 | S3 sink write failure |

## 8. Local mode — deviation from `AGENTS.md` §6 F-1

**This is an intentional, documented deviation.** `AGENTS.md` describes the production
topology only: EDGAR → S3 → Databricks. The current working mode is the interim one —
data is pulled to a local disk and loaded into tables by hand, because repo 2 (AWS
infra, SSM) is not stood up yet.

Rather than bypass the pipeline for that, a third sink is added:

- `LocalSink` writes the **same bytes** to
  `{local_landing_dir}/edgar/{stream}/dt={logical_date}/{batch_id}.json.gz`.
- The relative path and filename are taken from
  `names.landing_path("volume", ...)` and re-rooted — **not** recomputed. Repo 1
  remains the only place that knows how a landing path is spelled.
- `--local-only` runs `LocalSink` alone and relaxes config validation so `raw_bucket`
  and the Databricks credentials are not required.

What this deviates from: F-1 specifies `raw_bucket: str` as unconditionally required.
Under `--local-only` it is optional. Nothing else in the contract moves — `landing_mode`
remains `Literal["s3", "volume"]`, and the landing path and envelope are unchanged.

The payoff is that the manual load and the eventual automated load consume
byte-identical files, so the hand-loaded tables and the S3-replayed tables cannot
disagree. The byte-identity test covers all three sinks, not two.

## 9. Resumability for `company_concept`

`CONCEPT_SET` (15) × universe (25 locally, 500 in production) = 375 / 7,500 requests.
At 5 rps that is ~75 s locally and ~25 min in production. A crash at request 6,000
must not restart at zero.

Mechanism (`AGENTS.md` §6 F-4): NDJSON lines are appended to a local checkpoint file
keyed by `(stream, logical_date)` as each `(cik, concept)` pair completes. `--resume`
reads the completed pairs out of that file and skips them. One landing object is
uploaded at the end, containing all records. The checkpoint is deleted on success.

Because the checkpoint stores the envelopes themselves, a resumed run produces the
same landing object as an uninterrupted one — `_fetched_at` differs per record, but
that field is metadata and is explicitly excluded from keys and filenames (data
contracts §1).

## 10. What this repo must never do

- Define or duplicate a schema. Import from `edgar_lakehouse_contracts`.
- Acquire a Spark dependency.
- Reshape a payload: no date parsing, no case normalization, no dedup, no field
  renaming inside `payload`. Envelope fields only.
- Derive a filename from wall clock.
- Hardcode a bucket, host, ARN, or path.
