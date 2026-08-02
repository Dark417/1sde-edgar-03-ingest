# edgar-lakehouse — global rules (all five repos)

> This file governs every repo in the project. Each repo carries its own `AGENTS.md`
> with repo-specific instructions; when the two disagree, the repo file wins for that
> repo. When a repo file and the authoritative docs (`docs/00-design-doc.md`,
> `docs/02-data-contracts.md`) disagree, **stop and report the conflict** — do not pick
> a side silently.

## The project in one paragraph

A Databricks (AWS, Free Edition) medallion lakehouse over SEC EDGAR filings and XBRL
financial facts, split into five repos with one-directional dependencies:

| # | Repo | Role |
|---|---|---|
| 1 | `1sde-edgar-01-contracts` | Liquibase DDL + Python schema package `edgar_lakehouse_contracts` + schema-drift test |
| 2 | `1sde-edgar-02-infra` | Terraform: AWS + Databricks workspace objects + SSM interface |
| 3 | `1sde-edgar-03-ingest` | EDGAR → S3 (system of record) + Volume (transport); containerized batch CLI |
| 4 | `1sde-edgar-04-pipelines` | bronze → silver → gold → Parquet serving export (Databricks Jobs) |
| 5 | `1sde-edgar-05-serving` | FastAPI + DuckDB over the Parquet export; public demo UI |

Build order 1→2→3→4→5, with one documented backward edge: repo 2 creates the
catalog/schemas before repo 1's `liquibase update` can run.

## Cross-repo laws

1. **Repos depend only on repo 1's published wheel and repo 2's SSM parameters.**
   Never on each other's source. Never on `main` — always an exact pinned version
   (`==`, not `>=`).
2. **One owner per object.** Tables: Liquibase (repo 1). Catalog/schemas/volume/jobs:
   Terraform (repo 2). Landing objects: ingest (repo 3). Delta rows: pipelines
   (repo 4). Parquet export: repo 4, read by repo 5. If two repos would touch the
   same object, the design is wrong — stop.
3. **No hardcoded ARNs, hosts, bucket names, or paths** outside repos 1–2. Config
   resolution is `env var → SSM → fail with a message naming the missing key`.
4. **`cik` is a `STRING` everywhere, zero-padded to 10.** Never an int, in any repo.
5. **Determinism over convenience.** Batch ids, file names, and hashes derive from
   logical dates and sorted inputs — never wall clock, never `hash()`, never dict
   order.
6. **No secrets in git, images, or Terraform state.** Secrets Manager + runtime
   injection only. A PAT or AWS key in a repo is an immediate stop-and-fix.
7. **Free Edition constraints are design inputs, not annoyances:** serverless only,
   no account-level API, ≤5 concurrent job tasks, daily quota shutdown, no DLT.
   Anything that ignores them fails at runtime, not plan time.
8. **Idempotency is tested, not assumed.** Every repo has a "run it twice" test:
   same input twice → same state, byte- or row-identical.
9. **CI gates are grep-able and merciless:** forbidden dependencies and forbidden
   resources are enforced by grep/tests in CI, per repo file.
10. **When ambiguous, stop and ask.** No guessed schema, no `TODO` placeholders.
11. **Every AWS resource lives in `us-east-2`, and this is not a preference.** The
    Unity Catalog metastore is `metastore_aws_us_east_2`; verified live on
    2026-08-01 via `databricks metastores get`, which returns `region: us-east-2`
    and `global_metastore_id: aws:us-east-2:083f6670-cb9a-4058-9fd5-ed2fd09b1f15`.
    A workspace can only attach to the metastore in its own region, so the region
    is fixed by the workspace and is not ours to choose. Buckets, ECR, ECS, SSM
    and every `configure-aws-credentials` step belong there too — anything
    elsewhere is cross-region egress on every read, and SSM lookups simply fail
    with `ParameterNotFound` because parameters are regional. This file said
    `us-east-1` until 2026-08-01 and repos 1 and 3 inherited it; if you are about
    to write any other region, you are re-introducing that bug.

## Conventions

- Python 3.11, `ruff` + `mypy --strict`, pytest; coverage thresholds per repo file.
- Commits: conventional-ish, imperative mood, small.
- Docs: each repo carries `docs/00-design-doc.md` and `docs/02-data-contracts.md`
  copied from repo 1 (repo 1 is the source of truth for both).
- GitHub org/user: `Dark417`. AWS region: `us-east-2` (law 11 — fixed by the
  metastore, never change it). All five repos are public.
