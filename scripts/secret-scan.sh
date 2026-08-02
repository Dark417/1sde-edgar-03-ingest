#!/usr/bin/env bash
# Tier-1/Tier-2 leak gate (root AGENTS.md "Sensitive values").
# Runs in CI for every repo and is safe to run locally: ./scripts/secret-scan.sh
#
# Exit 1 on any hit. Scans tracked files only — untracked local files such as
# docs/LOCAL-VALUES.md, plan.txt and changelog/liquibase.properties are where
# real values are SUPPOSED to live.
set -uo pipefail

fail=0

# Obvious dummies in *.example / docs are fine and must stay readable:
# dapi000…, dapiXXXX…, <PLACEHOLDER>, "replace-me". Anything with real entropy
# is not filtered, so a genuine token in an .example file still fails.
PLACEHOLDER='dapi(0{8,}|[xX]{8,})|AKIAIOSFODNN7EXAMPLE|AKIA[X0]{16}|EXAMPLE|<[A-Z_]+>|replace[-_]me|your[-_]'

scan() { # name, extended-regex
  local name="$1" pattern="$2" hits
  # -I skips binaries; scan the committed tree, not the working dir
  hits=$(git grep -InIE "$pattern" -- . ':!*AGENTS*.md' ':!scripts/secret-scan.sh' \
         ':!docs/LOCAL-VALUES.example.md' 2>/dev/null \
         | grep -vE "$PLACEHOLDER" || true)
  if [ -n "$hits" ]; then
    echo "::error::$name"
    echo "$hits"
    fail=1
  fi
}

# --- Tier 1: real secrets. A hit means ROTATE the credential, then purge. ----
scan "Databricks PAT committed"      'dapi[0-9a-f]{24,}'
scan "AWS access key id committed"   '(A3T[A-Z0-9]|AKIA|ASIA)[A-Z0-9]{16}'
scan "Private key committed"         'BEGIN (RSA |EC |OPENSSH )?PRIVATE KEY'

# --- Tier 2: environment identifiers. Use a placeholder + resolution hint. ---
scan "AWS account id (use <AWS_ACCOUNT_ID>)"      '\b806168459926\b'
scan "Metastore id (use <METASTORE_ID>)"          '083f6670-cb9a-4058-9fd5-ed2fd09b1f15'
scan "Workspace id (use <DBX_WORKSPACE_ID>)"      'dbc-6e85f573-bc49'
scan "Warehouse id (use <WAREHOUSE_ID>)"          '\b733dc896c4c3751c\b'

# --- Files that must never be tracked ----------------------------------------
tracked_bad=$(git ls-files | grep -E '(^|/)(\.env|.*\.tfstate.*|liquibase\.properties|LOCAL-VALUES\.md|plan\.txt)$' || true)
if [ -n "$tracked_bad" ]; then
  echo "::error::Files that must never be committed are tracked:"
  echo "$tracked_bad"
  fail=1
fi

if [ "$fail" -eq 0 ]; then echo "secret-scan: clean"; fi
exit "$fail"
