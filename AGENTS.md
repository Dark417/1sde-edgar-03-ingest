# Repo 3 / 5 — `1sde-databricks-03-ingest`

> Copy to repo root as `AGENTS.md`. Sections 0–8 are agent instructions. Section 9 is
> yours, by hand. Section 10 is what repo 4 consumes.
>
> GitHub: `github.com/Dark417/1sde-databricks-03-ingest`
> Build order position: **3 of 5.** Requires repos 1 and 2 complete.

---

## 0. Read first

This repo fetches from SEC EDGAR and writes raw records to the landing zone. It is a
batch CLI packaged as a container, run by ECS on a schedule. It is deliberately the
only component that touches the public internet.

**Authoritative docs** in `docs/`: `00-design-doc.md` (§4.2 EDGAR constraints, §5.1
the dual-sink design, §8.1 idempotency), `02-data-contracts.md` (§0 streams, §1
landing envelope).

**The one design idea that governs this repo:** S3 is the system of record and commits
first; the Databricks landing push is a *transport* that is allowed to fail. Ingest is
never blocked by Databricks being down. Getting this backwards — failing the run
because a Volume `PUT` 500'd — defeats the entire replay story.

---

## 1. Scope

### Owns
- EDGAR HTTP client: rate limiting, retries, parsing.
- Landing envelope construction.
- Sinks: S3 (always) + Databricks Volume (conditional) + no-op.
- CLI, config resolution, structured logging.
- The container image.

### Does NOT own
- Any schema definition. Import it from `fin_lakehouse_contracts`.
- Any Spark. This repo has no Spark dependency and must not acquire one.
- Any transformation. Payloads are passed through **verbatim**. Reshaping raw data
  here destroys the property that makes replay meaningful.
- Any infrastructure. Repo 2 created the bucket, role, task, and schedule.
- Table creation. Repo 1's Liquibase did that.

### The boundary that will tempt you
"While I'm here I could parse the dates / normalize the company name / drop the
duplicate rows." **No.** Bronze does that. If this repo starts making semantic
decisions, a replay from raw no longer reproduces history — it reproduces whatever
this repo's logic was on the day it ran.

---

## 2. Prerequisites from repos 1 and 2

| Input | Source | How it reaches this repo |
|---|---|---|
| `fin_lakehouse_contracts==<version>` | repo 1 wheel | pinned in `pyproject.toml`; wheel fetched via `aws s3 cp` from the wheels prefix (pip cannot read `s3://` directly) |
| `Stream`, `landing_path()`, `batch_id()`, `LandingEnvelope`, `FilingIndexRecord` | repo 1 package | imported — **never reimplemented** |
| `ADR-001` landing mode | repo 1 `docs/` | `LANDING_MODE` env, default from SSM |
| `/fin-lakehouse/s3/raw_bucket` | repo 2 SSM | runtime config |
| `/fin-lakehouse/dbx/host`, `/fin-lakehouse/dbx/volume_path` | repo 2 SSM | runtime config |
| `/fin-lakehouse/ecr/ingest_repo` | repo 2 SSM | CI push target |
| Secrets `/fin-lakehouse/sec/user-agent`, `/fin-lakehouse/databricks/pat` | repo 2 (hand-created) | injected by ECS as `secrets` |
| OIDC role ARN for this repo | repo 2 SSM | CI auth |

**Hard rule:** no bucket name, host, ARN, or path is hardcoded. Config resolution
order is `env var → SSM → fail with a clear message naming the missing key`.

---

## 3. Tech baseline

```
Python      3.11
HTTP        httpx (sync client; this is a batch job, async buys nothing)
AWS         boto3
CLI         typer
Config      pydantic-settings
Logging     structlog (JSON to stdout)
Tests       pytest, respx, moto, freezegun
Lint/types  ruff, mypy --strict
Container   python:3.11-slim, multi-stage, uv, non-root
```

**Forbidden dependencies:** `pyspark`, `pandas`, `requests`, `databricks-sdk`
(the Files API is three lines of `httpx`; the SDK pulls a large dependency tree into
a container that runs for ninety seconds).

---

## 4. Layered structure

```
1sde-databricks-03-ingest/
├── AGENTS.md
├── pyproject.toml
├── Dockerfile
├── src/ingest/
│   ├── __init__.py
│   ├── config.py        # L0: settings, SSM resolution
│   ├── logging.py       # L0: structlog setup
│   ├── edgar/
│   │   ├── client.py    # L1: HTTP, rate limit, retry
│   │   ├── parsers.py   # L1: .idx fixed-width parser
│   │   └── errors.py    # L1: typed exceptions
│   ├── sinks/
│   │   ├── base.py      # L1: Sink protocol, SinkResult
│   │   ├── s3.py        # L2
│   │   ├── volume.py    # L2
│   │   └── noop.py      # L2
│   ├── streams/
│   │   ├── filing_index.py         # L3: orchestrates client -> envelope -> sinks
│   │   ├── company_submissions.py  # L3
│   │   └── company_concept.py      # L3
│   └── cli.py           # L4: entrypoint only
├── tests/
│   └── fixtures/        # committed real EDGAR samples
└── .github/workflows/ci.yml
```

**Layer rule:** L0 imports nothing internal. Each layer imports only layers below it.
`cli.py` contains no logic beyond argument parsing and calling a stream handler.

---

## 5. Non-negotiable rules for the agent

1. **`User-Agent` on every request, containing an `@`.** Raise at client construction
   if absent or malformed. The SEC rejects anonymous clients; failing at startup with
   a clear message beats 403s at 6 a.m.
2. **Rate limit default 5 rps, hard cap 8.** The SEC fair-access guidance is on the
   order of 10 req/s; running at half of it costs you nothing and a ban costs you the
   project. Token bucket, not `sleep(0.2)`.
3. **Retry 429 and 5xx only.** Exponential backoff with jitter, max 5 attempts.
   **Never retry a 403** — it means your UA is wrong and retrying makes it worse.
   Never retry a 404.
4. **404 from `company_concept` is not an error.** A company legitimately may not
   report a given concept. Return `None`, log at DEBUG, continue. Say this in the
   docstring.
5. **S3 write commits before the landing push is attempted.** If the landing push
   fails: log `LANDING_PUSH_FAILED` at ERROR, emit a metric, **exit 0**.
6. **Payloads pass through verbatim.** No date parsing, no case normalization, no
   dedup, no field renaming inside `payload`. Envelope fields only.
7. **Filenames derive from `batch_id`, never from wall clock.** Auto Loader's
   exactly-once guarantee is per file path; a timestamped filename makes a re-run look
   like new data and silently doubles bronze (design doc §8.1).
8. **The `.idx` file is fixed-width, not CSV.** Parse by column position. Validate the
   header line against an expected layout and **raise** if it does not match. Silently
   producing garbage rows from a changed format is the worst available outcome.
9. **Structured JSON logs to stdout.** One `ingest_complete` summary line per run with
   `stream, logical_date, batch_id, records, bytes, duration_s, sinks`.
10. **Exit codes are contract:** `0` success (including landing-push failure), `1`
    source fetch failure, `2` config error, `3` sink write failure to S3.
11. **No network in unit tests.** `respx` + committed fixtures. A test that hits
    sec.gov is a flaky test and will be deleted.
12. **`--dry-run` writes nothing.** Asserted by call-count tests, not by inspection.

---

## 6. Features to generate

### F-1 · `config.py`
```python
class Settings(BaseSettings):
    sec_user_agent: str
    landing_mode: Literal["s3", "volume"]
    raw_bucket: str
    dbx_host: str | None = None
    dbx_token: SecretStr | None = None
    volume_path: str = "/Volumes/fin/landing/edgar"
    max_rps: float = 5.0
    cik_universe_uri: str | None = None
```
Resolution: env → SSM (`/fin-lakehouse/*`) → error naming the missing key.
Validator: if `landing_mode == "volume"`, `dbx_host` and `dbx_token` are required.

**Acceptance:** missing `sec_user_agent` exits 2 with a message containing the env var
name. A UA without `@` fails validation.

### F-2 · `edgar/client.py`
```python
class EdgarClient:
    def __init__(self, user_agent: str, max_rps: float = 5.0,
                 client: httpx.Client | None = None) -> None: ...
    def fetch_daily_index(self, logical_date: date) -> Iterator[FilingIndexRecord]: ...
    def fetch_submissions(self, cik: str) -> dict[str, Any]: ...
    def fetch_company_concept(self, cik: str, taxonomy: str,
                              concept: str) -> dict[str, Any] | None: ...
```
URL construction: daily index lives under
`/Archives/edgar/daily-index/{year}/QTR{q}/form.{yyyymmdd}.idx`. Quarter derived from
the month. Weekend/holiday dates 404 — surface as a typed `NoIndexForDate` and let the
caller decide (the CLI treats it as success with zero records, because a Saturday has
no filings and that is not a failure).

**Acceptance**
- Fixture `form.idx` (~50 real lines, committed) parses to the expected count.
- Malformed header raises `IndexFormatChanged`, does not return rows.
- Rate limiter: 20 requests at `max_rps=5` takes ≥ 3.8 s with a fake clock.
- 403 is not retried (assert exactly one request made).
- 429 then 200 → two requests, success.

### F-3 · `sinks/`
```python
@dataclass(frozen=True)
class SinkResult:
    uri: str
    bytes_written: int
    record_count: int

class Sink(Protocol):
    def write(self, stream: Stream, logical_date: date,
              records: Iterable[LandingEnvelope]) -> SinkResult: ...
```
`S3Sink`: gzip NDJSON, key from `names.landing_path("s3", ...)`, `put_object` with
`ContentEncoding=gzip`. Overwrites on re-run — deterministic key means idempotent.

`VolumeSink`: `PUT {host}/api/2.0/fs/files{volume_path}/{stream}/dt={date}/{batch}.json.gz?overwrite=true`
with `Authorization: Bearer`. Stream the body; do not build one `bytes` for a large
concept batch.

**Acceptance**
- Both sinks produce byte-identical payloads for the same input (asserted directly).
- `VolumeSink` raising → process exit 0, S3 object present, one ERROR log containing
  `LANDING_PUSH_FAILED`.
- Re-running the same `(stream, logical_date)` writes to the same key.

### F-4 · `streams/`
One handler per stream. Each: fetch → wrap in `LandingEnvelope` → write to both sinks
→ return a summary.

`company_submissions` and `company_concept` iterate the CIK universe. Universe source:
`cik_universe_uri` (a Delta table export or a committed JSON list for MVP1). Support
`--cik-limit N` for cheap testing.

`company_concept` fans out over `CONCEPT_SET` × universe — that is 15 × 500 = 7,500
requests. At 5 rps that is ~25 minutes. **Design for resumability:** write one landing
object per (logical_date) containing all records, but checkpoint progress so a crash
at request 6,000 does not restart at zero. Simplest sufficient mechanism: write
partial NDJSON to a local temp file, upload once at the end, and record completed
`(cik, concept)` pairs in that temp file so a re-run with `--resume` skips them.

**Acceptance:** `--cik-limit 3` makes exactly `3 × len(CONCEPT_SET)` requests.
`--resume` after a simulated crash makes only the remaining requests.

### F-5 · `cli.py`
```
python -m ingest.cli run --stream {filing_index|company_submissions|company_concept}
                         --logical-date YYYY-MM-DD
                         [--dry-run] [--cik-limit N] [--resume]
python -m ingest.cli config-check
```
`--dry-run`: fetch, print a manifest (record count, first 3 records, both target
paths), write nothing.
`config-check`: resolve all config, print it with secrets redacted, exit 0/2. This is
what you run first inside a new ECS task to find out why it will fail.

**Acceptance:** unknown `--stream` exits 2 listing valid values. `--dry-run` makes zero
`put_object` and zero Files API calls.

### F-6 · `Dockerfile`
Multi-stage: builder installs with `uv` into a venv; runtime copies the venv onto
`python:3.11-slim`, runs as UID 1000. No `HEALTHCHECK` — this is a batch task, not a
service; add a comment saying so, because someone will "fix" its absence.

**Acceptance:** image < 250 MB; `docker run <img> config-check` exits 2 with a clear
message when env is empty.

---

## 7. Testing requirements

| Requirement | Threshold |
|---|---|
| Coverage | ≥ 85% |
| Network in tests | zero — `respx` + committed fixtures |
| Fixtures | real EDGAR samples, trimmed, committed under `tests/fixtures/` |
| Time | `freezegun` — no test depends on today's date |
| Idempotency | a test writes the same batch twice and asserts one object, same key |

**The test that matters most:** byte-identity between the two sinks. If they diverge,
your S3 replay produces different bronze than the live path did, and you will not find
out until you need the replay.

---

## 8. CI — `.github/workflows/ci.yml`

```
on: pull_request  -> ruff, mypy, pytest, contract-compat check
on: push main     -> above + docker build + push to ECR by digest
                     + update ECS task definition
```

**Contract-compat check:** load the pinned `fin_lakehouse_contracts` and assert every
field this repo writes into an envelope exists in that version's `LandingEnvelope`.
This is the mitigation for the five-repo split; without it, drift is silent until
production.

Auth: OIDC role from repo 2. No long-lived AWS keys, ever.

---

## 9. EXECUTION — what you do manually

### 9.1 Create the repo
```bash
gh repo create Dark417/1sde-databricks-03-ingest \
  --private --add-readme --gitignore Python --license mit --clone
cd 1sde-databricks-03-ingest
mkdir -p docs && cp ../design/00-design-doc.md ../design/02-data-contracts.md docs/
```

### 9.2 Pin the contracts version 🔴
```toml
# pyproject.toml
dependencies = ["fin-lakehouse-contracts==0.1.0", "httpx", "boto3", ...]
```
Exact pin, `==`, not `>=`. A caret range across five repos means five different
versions in production and no way to reason about which.

### 9.3 Collect an EDGAR fixture by hand
```bash
curl -H "User-Agent: fin-lakehouse-demo you@example.com" \
  "https://www.sec.gov/Archives/edgar/daily-index/2026/QTR3/form.20260729.idx" \
  | head -60 > tests/fixtures/form.20260729.idx
```
Do this yourself and eyeball it. The agent must not guess the fixed-width layout —
it has changed before and it will change again.

### 9.4 First dry run
```bash
export SEC_USER_AGENT="fin-lakehouse-demo you@example.com"
export LANDING_MODE=volume
export RAW_BUCKET=$(aws ssm get-parameter --name /fin-lakehouse/s3/raw_bucket \
  --query Parameter.Value --output text)
export DBX_HOST=$(aws ssm get-parameter --name /fin-lakehouse/dbx/host \
  --query Parameter.Value --output text)
export DBX_TOKEN=$(aws secretsmanager get-secret-value \
  --secret-id /fin-lakehouse/databricks/pat --query SecretString --output text)

python -m ingest.cli run --stream filing_index \
  --logical-date 2026-07-29 --dry-run
```

**Check by hand — this is the moment you catch a wrong parser:**
- [ ] record count in the low thousands. Zero or six digits means wrong file or wrong
      layout.
- [ ] spot-check three accession numbers on
      `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany`
- [ ] both target paths printed, same filename, different prefix

### 9.5 First real run
```bash
python -m ingest.cli run --stream filing_index --logical-date 2026-07-29
aws s3 ls "s3://$RAW_BUCKET/edgar/filing_index/dt=2026-07-29/"
```
Then confirm in a Databricks notebook:
```python
dbutils.fs.ls("/Volumes/fin/landing/edgar/filing_index/dt=2026-07-29/")
```

### 9.6 Run it twice
Run the exact same command again. `aws s3 ls` must show **one** object, not two. If
there are two, `batch_id` is not deterministic and everything downstream will double.
Fix before proceeding.

### 9.7 Build and push the image
```bash
ECR=$(aws ssm get-parameter --name /fin-lakehouse/ecr/ingest_repo \
  --query Parameter.Value --output text)
aws ecr get-login-password | docker login --username AWS --password-stdin "${ECR%%/*}"
docker build -t "$ECR:0.1.0" .
docker push "$ECR:0.1.0"
```

### 9.8 Run once on ECS by hand
Run the task manually from the console with `config-check` as the command override
first, then with a real `run`. Confirm CloudWatch logs show the `ingest_complete`
line. Only after this works does repo 2's schedule get enabled.

### 9.9 Backfill (optional, after repo 4 works)
Run from your laptop, not ECS — you want to be able to Ctrl-C it.
```bash
for d in $(python - <<'EOF'
import datetime
d=datetime.date(2026,1,2)
while d<datetime.date(2026,7,31):
    if d.weekday()<5: print(d)
    d+=datetime.timedelta(days=1)
EOF
); do
  python -m ingest.cli run --stream filing_index --logical-date "$d" || break
  sleep 2
done
```
Backfill ingest fully first (cheap, all in AWS), then run bronze/silver once over the
whole landing zone. Do not interleave — Free Edition shuts down compute for the rest
of the day if you exceed quota.

---

## 10. Published outputs — what repo 4 consumes

| Output | Form | Consumed by |
|---|---|---|
| Landing objects | gzip NDJSON at `landing_path(...)` | 4 (Auto Loader source) |
| Envelope shape | per repo 1 `LandingEnvelope` | 4 (bronze parses it) |
| `_batch_id`, `_logical_date`, `_schema_version` | envelope fields | 4 (bronze metadata columns) |
| Container image digest | ECR | 2 (task definition) |

**Contract with repo 4:** every landing object is gzip NDJSON, one envelope per line,
`payload` verbatim from the source. Repo 4 may assume nothing else — in particular it
may not assume payload fields are typed, present, or non-null.

---

## 11. Definition of done

- [ ] `ruff`, `mypy --strict`, `pytest` green; coverage ≥ 85%
- [ ] Zero network calls in the test suite
- [ ] Byte-identity test between S3 and Volume sinks passes
- [ ] Same-input-twice produces one object with the same key
- [ ] `VolumeSink` failure → exit 0 with S3 object present
- [ ] `--dry-run` provably writes nothing
- [ ] Image < 250 MB, runs non-root
- [ ] One successful manual ECS run visible in CloudWatch
- [ ] Landing objects visible from a Databricks notebook

---

## 12. References

1. SEC EDGAR REST APIs (submissions, companyconcept, frames) — https://www.sec.gov/edgar/sec-api-documentation
2. SEC fair access / User-Agent requirement — https://www.sec.gov/os/accessing-edgar-data
3. EDGAR full/daily index structure — https://www.sec.gov/Archives/edgar/daily-index/
4. Databricks Files API (Volume upload) — https://docs.databricks.com/api/workspace/files/upload
5. `respx` for httpx mocking — https://lundberg.github.io/respx/
6. `moto` for S3 mocking — https://docs.getmoto.org/en/latest/
