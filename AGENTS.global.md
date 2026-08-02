# edgar-lakehouse — global rules (all six repos)

> This file governs every repo in the project. Each repo carries its own `AGENTS.md`
> with repo-specific instructions; when the two disagree, the repo file wins for that
> repo. When a repo file and the authoritative docs (`docs/00-design-doc.md`,
> `docs/02-data-contracts.md`) disagree, **stop and report the conflict** — do not pick
> a side silently.

## How these rules reach an agent

This file is the **only** copy that is edited. It is propagated verbatim into
every repo as `AGENTS.global.md` by `scripts/sync-rules.sh`.

The mechanism matters, because it silently failed once: agents auto-load
`AGENTS.md` from whichever repo they are opened in, and **`AGENTS.global.md` is
not a filename anything discovers**. Copying the rules into six repos achieved
nothing on its own — a thread working inside a single repo never read them.

So two things are required, and both are checked:

1. **Every repo's `AGENTS.md` opens with a pointer** to `AGENTS.global.md`,
   marked as required reading. A repo without that pointer has no global rules.
2. **Every copy is byte-identical to this file.** `./scripts/sync-rules.sh
   --check` fails on drift or a missing pointer; run it after editing this file,
   and re-propagate with `./scripts/sync-rules.sh`.

Order of reading, for an agent starting in any repo: `AGENTS.global.md` →
that repo's `AGENTS.md` → the authoritative docs in `docs/`.

## The project in one paragraph

A Databricks (AWS, Free Edition) medallion lakehouse over SEC EDGAR filings and XBRL
financial facts, split into six repos with one-directional dependencies:

| # | Repo | Role |
|---|---|---|
| 1 | `1sde-edgar-01-contracts` | Liquibase DDL + Python schema package `edgar_lakehouse_contracts` + schema-drift test |
| 2 | `1sde-edgar-02-infra` | Terraform: AWS + Databricks workspace objects + SSM interface |
| 3 | `1sde-edgar-03-ingest` | EDGAR → S3 (system of record) + Volume (transport); containerized batch CLI |
| 4 | `1sde-edgar-04-pipelines` | bronze → silver → gold → Parquet serving export (Databricks Jobs) |
| 5 | `1sde-edgar-05-serving` | FastAPI + DuckDB over the Parquet export; public demo UI |
| 6 | `1sde-edgar-06-chatbot` | LangGraph agent answering natural-language questions over gold |

Build order 1→2→3→4→5→6, with one documented backward edge: repo 2 creates the
catalog/schemas before repo 1's `liquibase update` can run.

## Cross-repo laws

1. **Repos depend only on repo 1's published wheel and repo 2's SSM parameters.**
   Never on each other's source. Never on `main` — always an exact pinned version
   (`==`, not `>=`).
2. **One owner per object.** Tables: Liquibase (repo 1). Catalog, schemas,
   volumes, grants: Terraform (repo 2). **Databricks job definitions: repo 4's
   Asset Bundle.** Landing objects: ingest (repo 3). Delta rows: pipelines
   (repo 4). Parquet export: repo 4, read by repo 5. If two repos would touch the
   same object, the design is wrong — stop.

   *Jobs moved from repo 2 to repo 4 on 2026-08-02, and the reason is worth
   keeping.* The tempting analogy is ECS: repo 2 declares the task definition,
   repo 3 supplies the image, and they stay independent because the task
   definition references the image by a single URI. **A Databricks job has no
   such seam.** Its tasks name the package, the entry point, the parameters and
   the dependency edges — that is the code's interface, not a container to fill.
   Declaring it from repo 2 meant restating repo 4's internals across a repo
   boundary, and every field was wrong: wrong wheel name, wrong package, six
   invented entry points against a single real dispatcher, six tasks against
   four. The job was live and would have failed on first run. Asset Bundles
   exist so the task graph lives beside the code that implements it.
3. **No hardcoded ARNs, hosts, bucket names, or paths** outside repos 1–2. Config
   resolution is `env var → SSM → fail with a message naming the missing key`.
4. **`cik` is a `STRING` everywhere, zero-padded to 10.** Never an int, in any repo.
5. **Determinism over convenience.** Batch ids, file names, and hashes derive from
   logical dates and sorted inputs — never wall clock, never `hash()`, never dict
   order.
6. **No secrets in git, images, or Terraform state.** Secrets Manager + runtime
   injection only. A PAT or AWS key in a repo is an immediate stop-and-fix.
   See "Sensitive values" below — **these repos are public**.
7. **Free Edition constraints are design inputs, not annoyances:** serverless only,
   no account-level API, ≤5 concurrent job tasks, daily quota shutdown, no DLT.
   Anything that ignores them fails at runtime, not plan time.
8. **Idempotency is tested, not assumed.** Every repo has a "run it twice" test:
   same input twice → same state, byte- or row-identical.
9. **CI gates are grep-able and merciless:** forbidden dependencies and forbidden
   resources are enforced by grep/tests in CI, per repo file.
10. **When ambiguous, stop and ask.** No guessed schema, no `TODO` placeholders.
11. **Stay inside your repo. Never edit another repo to make your change work.**
    Multiple agents/threads work this project in parallel, one repo each. Editing
    across the boundary is how work gets clobbered, how two copies of the same
    contract appear, and how a repo's tests start depending on someone's
    uncommitted local state.

    **You may only write to the repo you were assigned.** For everything else:

    | Situation | Do this — not a cross-repo edit |
    |---|---|
    | You need a schema/envelope/name change | Open a **PR against repo 1**, or ask the user. Repo 1 ratifies all shape changes; consumers propose, they do not patch. |
    | Another repo looks wrong/broken | **Report it to the user.** Do not fix it. It may be deliberate or mid-flight. |
    | You need a value another repo owns | Read it from SSM / the published wheel / the release URL. Never reach into its source tree. |
    | You are tempted to copy code from another repo | Stop. That is vendoring, and it is the exact failure this five-repo split exists to prevent. Depend on the published artifact instead. |
    | The change genuinely spans repos | **Ask the user first**, then do it in one repo at a time, each with its own commit and green CI. |

    **Never commit or push another repo's working tree**, even if it looks
    finished — an uncommitted file may be a half-done thought, and pushing it
    publishes someone else's unfinished work and can turn their CI red.

    This rule is not theoretical: a concurrent session once generated repo 2 into
    a stale directory during a rename, and repo 4 vendored a private copy of
    repo 1's contracts — which then silently disagreed with repo 1 on all eleven
    envelope field names.
12. **End every chat response with BOTH summaries, in this order.** They do
    different jobs and neither replaces the other:

    **(a) Your own summary — write it however the content deserves.** Prose,
    a table, a diagram, whatever explains it best. This is where the reasoning,
    the caveats, the "here is why this was harder than it looked", and the
    things that do not fit a bucket belong. Do not flatten it to fit a template.

    **(b) The structured status block below.** Freeform writing loses things:
    an unanswered question slides away when the conversation moves on, and a
    blocker reads like a footnote. The block is the guarantee that nothing is
    dropped, and it is scannable in three seconds.

    Yes, this repeats information. That is the point — (a) is for understanding,
    (b) is for not missing anything. Keep (b) terse precisely because (a)
    already carried the nuance; do not re-explain, just state status.

    The status block has four headings, in this order, and a heading is omitted
    only when it is genuinely empty:

    - **Decisions needed from you** — anything the agent cannot or should not
      settle alone. Name the options and give a recommendation; do not just
      raise a question.
    - **Blocking / to fix** — what is broken right now, and whether it blocks
      the next step. Mark each as blocking or not; "found four issues" is
      useless if three are cosmetic.
    - **Previous round** — for each question or todo raised last turn: answered,
      done, or still open. Never let an unanswered question silently disappear
      because the conversation moved on.
    - **Remaining for this thread's goal** — the todos still standing between
      here and this thread's objective, not a general backlog.

    **Every bullet starts with a status emoji**, so severity is readable without
    parsing the sentence:

    | Emoji | Means |
    |---|---|
    | ✅ | done and verified — not "ran without error", but checked |
    | ❌ | broken **and blocking** the next step |
    | ⚠️ | wrong or risky, **not** blocking — a cosmetic defect and a data-loss risk must not look alike |
    | ❓ | open question or decision awaiting the user |
    | ⏳ | in progress, or waiting on something external (a CI run, an AWS bootstrap, another thread) |
    | 🔵 | queued and not started — a todo with nothing wrong with it |

    Never use ✅ for something merely attempted. Verify claims before they enter
    this block: a status line asserting something is done, when it was only
    attempted, is worse than no status line at all.
13. **Every AWS resource lives in `us-east-2`, and this is not a preference.** The
    Unity Catalog metastore is `metastore_aws_us_east_2`; verified live on
    2026-08-01 via `databricks metastores get`, which returns `region: us-east-2`
    and `global_metastore_id: aws:us-east-2:<METASTORE_ID>`.
    A workspace can only attach to the metastore in its own region, so the region
    is fixed by the workspace and is not ours to choose. Buckets, ECR, ECS, SSM
    and every `configure-aws-credentials` step belong there too — anything
    elsewhere is cross-region egress on every read, and SSM lookups simply fail
    with `ParameterNotFound` because parameters are regional. This file said
    `us-east-1` until 2026-08-01 and repos 1 and 3 inherited it; if you are about
    to write any other region, you are re-introducing that bug.

## Sensitive values — these repos are PUBLIC

Every repo is public on GitHub. Assume anything committed is permanently
readable by anyone, including git history. Three tiers, and the tier decides
the handling:

### Tier 1 — SECRETS. Never in git, in any form, ever.
PATs (`dapi…`), AWS access keys (`AKIA…`), passwords, private keys, session
tokens, `*.tfstate`, `liquibase.properties`, `.env`.
- Storage: AWS Secrets Manager (runtime) or a **gitignored** local file.
- CI: GitHub Actions secrets, referenced as `${{ secrets.NAME }}`.
- A leak is not fixed by deleting the line — **rotate the credential first**,
  then purge. Assume it was scraped within minutes of the push.

### Tier 2 — IDENTIFIERS. Masked in committed text; resolved at runtime.
AWS account id, UC metastore id, workspace host, warehouse id, ARNs, ECR URIs,
concrete bucket names. Not secret on their own, but they are free targeting
information and they pin the project to one person's tenancy.
- **In code/config:** never literals — Terraform variables, env vars, or SSM
  lookups. Config resolution stays `env var → SSM → fail naming the key`.
- **In docs, comments, and examples:** write a placeholder **plus how to
  resolve it**, so the doc stays runnable without carrying the value:

  | Use | Not |
  |---|---|
  | `<AWS_ACCOUNT_ID>` (`aws sts get-caller-identity --query Account`) | a literal 12-digit account id |
  | `<DBX_HOST>` (SSM `/edgar-lakehouse/dbx/host`) | `dbc-xxxxxxxx-xxxx.cloud.databricks.com` |
  | `<WAREHOUSE_ID>` (SQL Warehouses → Connection details) | a literal hex id |
  | `<METASTORE_ID>` (`databricks metastores get`) | a literal uuid |

- **Never lose the reference:** the real values live in
  `docs/LOCAL-VALUES.md`, which is **gitignored** in every repo and whose
  template (`docs/LOCAL-VALUES.example.md`) is committed. Every placeholder in
  a doc must be resolvable either from that file or from the command named
  beside it.

### Tier 3 — PUBLIC. Commit freely.
Catalog/schema/table names, bucket *name patterns* (`edgar-lake-raw`), region
`us-east-2`, package/version strings, the SEC contact email (SEC *requires* a
real contact in the User-Agent, and it is already the git commit author),
architecture docs, ADRs. Masking these buys nothing and breaks the docs.

### Enforcement
- Every repo runs a **secret-scan gate in CI** (`make secret-scan` equivalent:
  a grep for Tier-1 patterns and known Tier-2 literals) and fails the build on
  a hit.
- GitHub **secret scanning + push protection** is enabled on every repo (free
  for public repos) — it blocks the push rather than the review.
- `.gitignore` in every repo covers: `docs/LOCAL-VALUES.md`, `*.tfstate*`,
  `.env`, `changelog/liquibase.properties`, `*.pem`, `*.key`.
- Before making any new repo public, run the scan over **full history**
  (`git grep <pattern> $(git rev-list --all)`), not just the working tree.

## Conventions

- Python 3.11, `ruff` + `mypy --strict`, pytest; coverage thresholds per repo file.
- Commits: conventional-ish, imperative mood, small.
- Docs: each repo carries `docs/00-design-doc.md` and `docs/02-data-contracts.md`
  copied from repo 1 (repo 1 is the source of truth for both).
- GitHub org/user: `Dark417`. AWS region: `us-east-2` (law 11 — fixed by the
  metastore, never change it). All five repos are public.
