# Local workflow — pull a subset, load it into tables by hand

The production path is EDGAR → S3 → Databricks Auto Loader. Repo 2's infrastructure
(bucket, ECR, ECS, SSM) has been applied, but the ECS schedule is disabled and the
remote path has not yet had its first validated run. This document describes the
interim path currently in use: **pull a small subset to local disk, then load it into
tables manually.**

The files produced locally are byte-identical to what the production path will write,
so the tables you hand-load now and the tables Auto Loader builds later cannot
disagree.

---

## 0. One-time setup

```bash
# Python 3.11 + uv
uv venv --python 3.11
source .venv/bin/activate

# repo 1's contracts wheel — from its GitHub release (not on PyPI); the tag must
# match the pin in pyproject.toml
gh release download v1.1.0 --repo Dark417/1sde-edgar-01-contracts \
  --pattern 'edgar_lakehouse_contracts-*.whl' --dir wheels
uv pip install --find-links wheels -e ".[dev]"
```

Exactly one thing is ever required:

```bash
export SEC_USER_AGENT="edgar-lakehouse-demo you@example.com"   # must contain an @
```

Local is the default mode, so there is nothing else to set: no AWS credentials,
no SSM parameters, no bucket. `LOCAL_LANDING_DIR` overrides the output root
(default `./local-landing`).

The SEC 403s anonymous clients. A `User-Agent` without an `@` fails at startup rather
than at the first request.

Confirm before fetching anything:

```bash
python -m ingest.cli config-check
```

Exits 0 with the resolved config printed (secrets redacted), or exits 2 naming the
missing key.

---

## 1. Pull the subset

`--logical-date` must be a business day; weekends and holidays legitimately 404 and
are reported as zero records, not as a failure.

```bash
# ~6k filings, one HTTP request, a couple of seconds
python -m ingest.cli run --stream filing_index --logical-date 2026-07-29

# 25 companies from the packaged universe, one request each
python -m ingest.cli run --stream company_submissions --logical-date 2026-07-29 \
  --cik-limit 25

# 3 companies x 15 concepts = 45 requests, ~9 s at 5 rps
python -m ingest.cli run --stream company_concept --logical-date 2026-07-29 \
  --cik-limit 3
```

Add `--dry-run` first if you want the manifest (record count, first 3 records, target
paths) without writing anything.

Output:

```
local-landing/edgar/filing_index/logical_date=2026-07-29/filing_index-20260729-<hash>.json.gz
local-landing/edgar/company_submissions/logical_date=2026-07-29/company_submissions-20260729-<hash>.json.gz
local-landing/edgar/company_concept/logical_date=2026-07-29/company_concept-20260729-<hash>.json.gz
```

Each file is gzip NDJSON, one landing envelope per line (data contracts §1).

Inspect:

```bash
gzip -dc local-landing/edgar/filing_index/logical_date=2026-07-29/*.json.gz | head -1 | python -m json.tool
gzip -dc local-landing/edgar/filing_index/logical_date=2026-07-29/*.json.gz | wc -l
```

---

## 2. Check it before you load it

This is the step that catches a wrong parser. Do it by hand the first time.

- [ ] `filing_index` record count is in the low thousands. Zero or six digits means
      the wrong file or the wrong column layout.
- [ ] Spot-check three accession numbers (`resource_id`, also inside `payload_json`)
      against `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany`.
- [ ] `cik` values inside `payload_json` are strings and keep their leading zeros
      where present.
- [ ] `logical_date` is `2026-07-29`, not a datetime.
- [ ] Run the exact same command twice. The directory must still contain **one**
      file, not two. Two files means `batch_id` is not deterministic and everything
      downstream will double (design doc §8.1).

---

## 3. Load into tables by hand

The landing files carry the envelope, not the table shape. Bronze (repo 4) is what
unwraps them. Until repo 4 runs, load them manually.

### Option A — upload to a Databricks Volume, then read

```bash
databricks fs cp -r local-landing/edgar \
  dbfs:/Volumes/edgar/landing/edgar --overwrite
```

The local layout is deliberately identical to the Volume layout, so this is a
straight copy — no path rewriting.

Then, in a notebook:

```python
from pyspark.sql import functions as F

# The envelope is flat (envelope.py in repo 1): metadata fields plus payload_json,
# which is the verbatim source record as one JSON string. Bronze columns and their
# mapping are from data contracts §6 (the v1.0.0 table reference).
PAYLOAD_DDL = (
    "company_name STRING, form_type STRING, cik STRING, "
    "date_filed STRING, file_name STRING, accession_number STRING"
)

raw = spark.read.json("/Volumes/edgar/landing/edgar/filing_index/logical_date=2026-07-29/")

bronze = (
    raw.withColumn("payload", F.from_json("payload_json", PAYLOAD_DDL))
    .select(
        F.to_date("logical_date").alias("logical_date"),
        F.col("resource_id"),
        F.to_timestamp("fetched_at").alias("fetched_at"),
        F.col("payload.form_type").alias("form_type"),
        F.col("payload.company_name").alias("company_name"),
        F.col("payload.cik").alias("cik"),
        F.col("payload.date_filed").alias("date_filed"),
        F.col("payload.accession_number").alias("accession_number"),
        F.col("payload.file_name").alias("file_name"),
        F.col("batch_id").alias("_ingest_batch_id"),
        F.current_timestamp().alias("_ingest_ts"),
        F.input_file_name().alias("_source_file"),
        F.col("source_system").alias("_source_system"),
        F.col("envelope_version").alias("_envelope_version"),
        F.lit(None).cast("string").alias("_rescued_data"),
    )
)
bronze.write.mode("append").saveAsTable("edgar.bronze.filing_index_raw")
```

Columns and types are from data contracts §6 (the v1.0.0 table reference, which
supersedes §2). Payload fields land as `STRING` — typing happens in silver, not here.

`company_submissions` and `company_concept` are simpler: the envelope's `payload_json`
string goes into bronze as-is (data contracts §2.2, §2.3), because those documents are
deeply nested and their shape is not ours to control. The CIK is the envelope's
`resource_id` (for `company_concept` it is `"<cik>/<concept>"` — split on `/`).

```python
raw = spark.read.json("/Volumes/edgar/landing/edgar/company_submissions/logical_date=2026-07-29/")
(raw.select(
    F.to_date("logical_date").alias("logical_date"),
    F.col("resource_id"),
    F.to_timestamp("fetched_at").alias("fetched_at"),
    F.col("resource_id").alias("cik"),
    F.col("payload_json"),
    F.col("batch_id").alias("_ingest_batch_id"),
    F.current_timestamp().alias("_ingest_ts"),
    F.input_file_name().alias("_source_file"),
    F.col("source_system").alias("_source_system"),
    F.col("envelope_version").alias("_envelope_version"),
    F.lit(None).cast("string").alias("_rescued_data"),
 ).write.mode("append").saveAsTable("edgar.bronze.company_submissions_raw"))
```

### Option B — DuckDB, entirely local, no Databricks

Useful for eyeballing the data without spending Free Edition quota.

```sql
SELECT (payload_json::JSON)->>'form_type'    AS form_type,
       (payload_json::JSON)->>'company_name' AS company_name,
       (payload_json::JSON)->>'cik'          AS cik,
       count(*) OVER ()                      AS total_rows
FROM read_json_auto('local-landing/edgar/filing_index/logical_date=2026-07-29/*.json.gz')
LIMIT 10;
```

**Append-only.** Bronze is append-only by design (design doc §5.2). If you load the
same batch twice by hand you will get duplicate rows — the deterministic filename
prevents Auto Loader from doing that, but it cannot prevent *you* from doing it.
Truncate and reload rather than appending twice.

---

## 4. Switching to the real path later

Nothing here is throwaway. When repo 2 is up:

```bash
export LOCAL_ONLY=false          # leaving local mode is always explicit
export LANDING_MODE=volume
export RAW_BUCKET=$(aws ssm get-parameter --name /edgar-lakehouse/s3/raw_bucket \
  --query Parameter.Value --output text)
export DBX_HOST=$(aws ssm get-parameter --name /edgar-lakehouse/dbx/host \
  --query Parameter.Value --output text)
export DBX_TOKEN=$(aws secretsmanager get-secret-value \
  --secret-id /edgar-lakehouse/databricks/pat --query SecretString --output text)

python -m ingest.cli run --stream filing_index --logical-date 2026-07-29 --remote
```

The same command now writes to S3 (system of record, commits first) and pushes to
the Volume (transport, allowed to fail). The bytes and the filename are the same
ones you loaded by hand.

**Set `LOCAL_ONLY=false` in the ECS task definition**, not just in your shell.
Local is the default, so a task that never sets it would land to a
container-local disk that vanishes when the task exits. Two things make that
hard to do by accident: the mode is logged as `landing_target` on every run, and
once `LOCAL_ONLY=false` is set, a missing `RAW_BUCKET` is a hard exit 2 rather
than a quiet downgrade. Run `config-check` as a command override first
(`AGENTS.md` §9.8) and read the `local_only` field before the first real run.
