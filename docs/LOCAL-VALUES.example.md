# Local values — template

Copy to `docs/LOCAL-VALUES.md` (**gitignored**) and fill in. This is the
"don't lose the reference" file: every `<PLACEHOLDER>` in the committed docs
resolves here, or via the command listed beside it.

Nothing in this file is a Tier-1 secret — secrets live in AWS Secrets Manager
or in `changelog/liquibase.properties` (also gitignored). These are Tier-2
identifiers: harmless to you, but not published, because the repos are public.

| Placeholder | Your value | How to re-derive it |
|---|---|---|
| `<AWS_ACCOUNT_ID>` | | `aws sts get-caller-identity --query Account --output text --profile edgar` |
| `<DBX_WORKSPACE_ID>` | | the `dbc-xxxxxxxx-xxxx` part of your workspace URL |
| `<DBX_HOST>` | | `https://<DBX_WORKSPACE_ID>.cloud.databricks.com`, or SSM `/edgar-lakehouse/dbx/host` |
| `<WAREHOUSE_ID>` | | SQL Warehouses → your warehouse → Connection details → the id in the HTTP path |
| `<METASTORE_ID>` | | `databricks metastores get` → `global_metastore_id` |
| `<TF_STATE_BUCKET>` | | `edgar-lakehouse-tfstate-<AWS_ACCOUNT_ID>` |

Secrets (never written here — listed so you know where they live):

| Secret | Location |
|---|---|
| Databricks PAT | Secrets Manager `/edgar-lakehouse/databricks/pat`; local copy only in `changelog/liquibase.properties` |
| SEC User-Agent | Secrets Manager `/edgar-lakehouse/sec/user-agent` |
| CI credentials | GitHub Actions secrets `DBX_HOST`, `DBX_HTTP_PATH`, `DBX_PAT` |
