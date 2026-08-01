# Design Doc — edgar lakehouse

> **Authoritative.** The five repo `AGENTS.md` files derive from this document and
> `02-data-contracts.md`. Conflicts are reported, not resolved silently.

## §1 Problem

Financial reference data has **mutable history**: a 10-K/A restates a prior period.
Most pipelines overwrite and lose the audit trail. This project models
`financial_fact` bitemporally at a grain that includes the asserting accession, which
makes restatement detection a query rather than a reconciliation job.

The deliverable is a working, demoable, end-to-end Databricks lakehouse on AWS:
EDGAR → landing → bronze → silver → gold → Parquet export → public read API + UI.

## §2 Goals / non-goals

**Goals:** correct medallion modeling; restatement detection as the flagship feature;
strict repo boundaries with semver'd contracts; everything reproducible from the raw
zone; near-zero cost (Databricks Free Edition + AWS free tier + always-on free PaaS).

**Non-goals / demo-scale decisions:** CIK universe bounded at 500 companies; daily
batch cadence (restatements detected up to 24 h late); one environment (`dev` by
catalog prefix); no auth on the read API; no streaming.

## §3 Architecture

```
            ┌────────────────────────  AWS  ────────────────────────┐
 SEC EDGAR ─► ECS Fargate (repo 3 CLI) ─► S3 edgar-lake-raw  (system of record)
            │        │                                              │
            │        └────────► Databricks Volume (transport, may fail)
            │                        │                              │
            │   Databricks Free Edition (repo 4 jobs)               │
            │   Auto Loader ─► bronze ─► silver ─► gold ─► export ──► S3 edgar-lake-serving
            └────────────────────────────────────────────────────────┘
                                                                     │
                              Fly/Render (repo 5): FastAPI + DuckDB ◄┘ ─► public UI
```

## §4 Constraints

### §4.1 Databricks Free Edition

These are design inputs, not annoyances:

1. **No account-level API.** Metastore, workspace, and account objects cannot be
   managed by Terraform. Only workspace-level resources exist (catalog, schema,
   volume, grants, jobs). Anything `databricks_mws_*` / `databricks_metastore*` /
   `databricks_account_*` fails at runtime, not plan time — repo 2 CI greps for them.
2. **Serverless compute only.** No cluster config, no init scripts, no JVM libraries,
   Python + SQL only.
3. **One active Lakeflow pipeline per type** → no DLT / Declarative Pipelines.
   Everything is a plain Job with explicit DQ code.
4. **Max 5 concurrent job tasks.** The task graph stays narrow (≤3 designed).
5. **Daily quota; exhaustion shuts compute down for the rest of the day.** Jobs fail
   fast, never retry blindly, `max_concurrent_runs: 1`.
6. **Databricks Apps auto-stop 24 h after start/redeploy** → the public demo cannot
   live on Databricks (see §5.4).
7. **Cloud-credential passthrough is not guaranteed** on this tier → the dual-sink
   design (§5.1).
8. **Not licensed for commercial use.** Stated in the front-door README.

### §4.2 EDGAR

1. Fair-access guidance ≈ 10 req/s; we default to 5, hard cap 8, token bucket.
2. Every request carries a `User-Agent` with a real contact email; the SEC 403s
   anonymous clients. A 403 is never retried.
3. The daily index is **fixed-width**, not CSV; the layout has changed before. Parse
   by position, validate the header, raise on mismatch.
4. Weekend/holiday dates 404 — that is "zero filings", not an error.
5. A company may legitimately lack a concept → `company_concept` 404 returns `None`.

## §5 Key design decisions

### §5.1 Dual sink: S3 commits first, Volume push may fail

S3 (`edgar-lake-raw`) is the **system of record** and commits first. The push to the
Databricks Volume is a *transport* that is allowed to fail (`LANDING_PUSH_FAILED`,
exit 0). Ingest is never blocked by Databricks being down. Both sinks write
byte-identical payloads to the same filename, so a replay from S3 reproduces exactly
what the live path produced. `ADR-001` in repo 1 records which mode Auto Loader reads
from (`s3` or `volume`).

### §5.2 Medallion with append-only bronze

Bronze is append-only, verbatim payloads plus six metadata columns; it is what we
replay from. Silver types, dedups, MERGEs (SCD-1 for filings/facts, SCD-2 for
company), executes DQ, quarantines rejects. Gold is query-shaped marts. Nothing
south of bronze reshapes raw data.

### §5.3 Five repos, one-directional dependencies

Five deploy targets with independent cadences and different blast radii. The cost is
that schema changes are not atomic — mitigated by a semver'd contracts package
(repo 1) that every consumer pins exactly and checks compatibility against in CI, and
a mechanical schema-drift test between the Liquibase changelogs and the Python
schemas.

Ownership: tables → Liquibase (repo 1); catalog/schemas/volume/jobs/IAM → Terraform
(repo 2); landing objects → repo 3; Delta rows → repo 4; serving Parquet → written by
repo 4, read by repo 5. One owner per object, no exceptions.

### §5.4 Serving never talks to Databricks

Free Edition compute can disappear for the rest of the day (§4.1.5) and Apps
auto-stop (§4.1.6). The demo link must survive both, so repo 5 reads Parquet from S3
via DuckDB and has **no runtime dependency on Databricks at all**. A `databricks`
import in repo 5 is a design failure, enforced by test and CI grep.

## §6 Repo layout

| # | Repo | Owns | Publishes |
|---|---|---|---|
| 1 | `1sde-databricks-edgar-01-contracts` | DDL changelogs, `edgar_lakehouse_contracts`, drift test | wheel + `CONTRACTS_VERSION` |
| 2 | `1sde-databricks-edgar-02-infra` | Terraform: AWS + workspace objects | SSM `/edgar-lakehouse/*` |
| 3 | `1sde-databricks-edgar-03-ingest` | EDGAR client, sinks, container | landing objects, image |
| 4 | `1sde-databricks-edgar-04-pipelines` | bronze/silver/gold jobs, export | serving Parquet + manifest |
| 5 | `1sde-databricks-edgar-05-serving` | FastAPI + DuckDB + UI | public URL |

Build order 1→2→3→4→5 with one backward edge: repo 2 creates catalog/schemas, then
repo 1's `liquibase update` runs. Config handoff is SSM Parameter Store, never
`terraform_remote_state`. Cross-repo code handoff is the pinned contracts wheel only.

## §7 Security

- No secrets in git, images, or Terraform state. Secrets Manager + runtime injection.
- CI → AWS via GitHub OIDC roles (one per repo, `sub`-scoped), no long-lived keys.
- ECS task role: least privilege, literally (`s3:PutObject` on the raw prefix; no
  delete — the raw zone is immutable).
- Raw bucket denies `s3:DeleteObject` except to the Terraform role.
- Serving reads S3 with a dedicated read-only IAM user.

## §8 Correctness properties

### §8.1 Idempotency and replay

- `batch_id` and filenames derive from `(stream, logical_date)` — never wall clock.
  Auto Loader's exactly-once guarantee is per file path; deterministic names make
  re-runs overwrite instead of duplicate.
- Bronze re-processing a landing file adds zero rows (checkpoint).
- Silver run twice → identical row count and identical `_first_seen_ts`.
- The raw zone is immutable; bronze→gold can be rebuilt from it at any time, and the
  result matches what the live path produced (byte-identical dual sinks).

### §8.2 Schema evolution

- Auto Loader `schemaEvolutionMode=rescue`; unexpected source fields land in
  `_rescued_data`. Non-null `_rescued_data` is a WARN metric, not a silent pass.
- Table DDL changes are Liquibase changesets, append-only, each with explicit
  rollback. Rollout order for breaking changes: contracts → pipelines → ingest →
  serving (expand → migrate → contract, see `MIGRATION.md`).

### §8.3 Data quality

Three severities, declared in the contracts DQ registry, executed by repo 4:
`reject` quarantines the row; `warn` emits a metric and keeps the row; `reject_batch`
fails the job (SCD-2 structural invariants). Every check names the concrete failure
it prevents (`prevents` field, enforced ≥20 chars); metrics are emitted even when
zero rows fail — a silent zero is indistinguishable from a check that never ran.

## §9 Operations

- One daily EventBridge schedule (created disabled; enabled only after a manual
  end-to-end run), 06:00 UTC.
- Budget alarm at $10/month before first apply.
- Weekly: click the demo link, check manifest freshness. Rotate the PAT every 90 days.
- Free-tier things die quietly; the health check reports *freshness*, not liveness.
