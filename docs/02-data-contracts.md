# Data Contracts — edgar lakehouse

> **Authoritative.** Every column, type, and nullability in the five repos comes from
> this document. Code that disagrees with this document is wrong; if this document is
> wrong, change it here first and bump `edgar_lakehouse_contracts` per `MIGRATION.md`.
>
> Type discipline (applies everywhere):
> - `cik` is `STRING`, zero-padded to 10 characters. Never an integer — leading zeros
>   are semantically meaningful in EDGAR URLs.
> - Timestamps are UTC `TIMESTAMP`. Dates are `DATE`. No string dates south of bronze.
> - Money/fact values are `DECIMAL(38,6)`.
> - Catalog is `edgar`; schemas are `landing`, `bronze`, `silver`, `gold`.

> ## ⚠️ v1.0.0 — authoritative table shapes
>
> The per-table column lists in sections 2–4 below describe the **v0.1.0** shape
> and are retained for history. The tables that exist in the workspace as of
> **v1.0.0** are listed in [§6 Table reference](#6-table-reference-v100) at the
> end of this document, which is generated from the same specs the Liquibase
> changesets and the Spark `StructType`s are built from and is verified by
> `tests/test_schema_drift.py`. Where 2–4 and §6 disagree, **§6 wins.**

---

## §0 Streams and concepts

### §0.1 Streams

| Stream | Source | Cadence | Payload |
|---|---|---|---|
| `filing_index` | `sec.gov/Archives/edgar/daily-index/{year}/QTR{q}/form.{yyyymmdd}.idx` | daily (business days) | fixed-width index rows |
| `company_submissions` | `data.sec.gov/submissions/CIK{cik10}.json` | daily over the CIK universe | JSON document, verbatim |
| `company_concept` | `data.sec.gov/api/xbrl/companyconcept/CIK{cik10}/us-gaap/{concept}.json` | daily over universe × concept set | JSON document, verbatim |

The CIK universe is bounded at 500 companies for the demo (a deliberate demo-scale
decision, documented in the design doc §2).

### §0.2 `CONCEPT_SET` — the 15 us-gaap concepts

```
Revenues
RevenueFromContractWithCustomerExcludingAssessedTax
NetIncomeLoss
OperatingIncomeLoss
GrossProfit
CostOfRevenue
Assets
AssetsCurrent
Liabilities
LiabilitiesCurrent
StockholdersEquity
CashAndCashEquivalentsAtCarryingValue
NetCashProvidedByUsedInOperatingActivities
EarningsPerShareBasic
EarningsPerShareDiluted
```

### §0.3 Canonical concept map (MVP2)

Exactly one canonical target exists in MVP2:

| Source concept (as filed) | `concept_canonical` |
|---|---|
| `Revenues` | `revenue_total` |
| `RevenueFromContractWithCustomerExcludingAssessedTax` | `revenue_total` |

Coalescing these two is best-effort; the as-filed `concept` is always retained
alongside `concept_canonical`. Concepts with no mapping carry
`concept_canonical = NULL`.

---

## §1 Landing envelope

Every landing object is **gzip NDJSON, one envelope per line**. The `payload` is
verbatim from the source — no parsing, no renaming, no dedup at landing.

| Field (JSON key) | Type | Null | Notes |
|---|---|---|---|
| `_stream` | string | no | one of the three stream names |
| `_logical_date` | string `YYYY-MM-DD` | no | the batch's logical date, never a datetime |
| `_batch_id` | string | no | deterministic, from `names.batch_id(stream, logical_date)` |
| `_fetched_at` | string, ISO-8601 UTC | no | wall-clock fetch time (metadata only — never used in keys or filenames) |
| `_source_url` | string | no | exact URL fetched |
| `_schema_version` | string | no | envelope schema semver, currently `"1"` |
| `payload` | object or array | no | verbatim source record |

Landing paths (same filename in both modes, only the prefix differs):

```
s3://{raw_bucket}/edgar/{stream}/dt={logical_date}/{batch_id}.json.gz
/Volumes/edgar/landing/edgar/{stream}/dt={logical_date}/{batch_id}.json.gz
```

---

## §2 Bronze

All bronze tables are **append-only** and carry the six metadata columns:

| Column | Type | Null |
|---|---|---|
| `_source_file` | STRING | no |
| `_ingest_ts` | TIMESTAMP | no |
| `_batch_id` | STRING | no |
| `_logical_date` | DATE | no |
| `_schema_version` | STRING | no |
| `_rescued_data` | STRING | yes |

### §2.1 `edgar.bronze.filing_index_raw`

Payload fields pass through as **STRING** (typing happens in silver):

| Column | Type | Null |
|---|---|---|
| `company_name` | STRING | yes |
| `form_type` | STRING | yes |
| `cik` | STRING | yes |
| `date_filed` | STRING | yes |
| `file_name` | STRING | yes |
| + six metadata columns | | |

### §2.2 `edgar.bronze.company_submissions_raw`

| Column | Type | Null |
|---|---|---|
| `cik` | STRING | yes |
| `payload_json` | STRING | yes |
| + six metadata columns | | |

`payload_json` is the whole submissions document as one string — the document is
deeply nested and its shape changes; exploding at bronze couples us to a shape we do
not control.

### §2.3 `edgar.bronze.company_concept_raw`

| Column | Type | Null |
|---|---|---|
| `cik` | STRING | yes |
| `taxonomy` | STRING | yes |
| `concept` | STRING | yes |
| `payload_json` | STRING | yes |
| + six metadata columns | | |

---

## §3 Silver

Every silver write is a `MERGE` on the business key. `_first_seen_ts` is set on insert
and never updated; `_last_seen_ts` updates on every merge.

Each silver table has a quarantine twin (`{table}_quarantine`, §3.4).

### §3.1 `edgar.silver.filing`

Business key: `accession_number`.

| Column | Type | Null | Notes |
|---|---|---|---|
| `accession_number` | STRING | no | normalized `0001234567-26-000123` |
| `cik` | STRING | no | zero-padded 10 |
| `company_name` | STRING | yes | |
| `form_type` | STRING | no | as filed, e.g. `10-K/A` |
| `base_form_type` | STRING | no | `form_type` with `/A` stripped, uppercased |
| `is_amendment` | BOOLEAN | no | `form_type` ends `/A` |
| `filed_date` | DATE | no | |
| `source_file_name` | STRING | yes | EDGAR index `file_name` field |
| `_first_seen_ts` | TIMESTAMP | no | insert-only |
| `_last_seen_ts` | TIMESTAMP | no | updated every merge |
| `_batch_id` | STRING | no | batch that last touched the row |

**DQ checks (silver.filing):**

| name | severity | expression (True = row is good) | prevents |
|---|---|---|---|
| `filing_accession_format` | reject | `accession_number RLIKE '^[0-9]{10}-[0-9]{2}-[0-9]{6}$'` | Malformed accession numbers breaking sec.gov links and the join to financial_fact |
| `filing_cik_padded` | reject | `cik RLIKE '^[0-9]{10}$'` | Unpadded CIKs splitting one company across multiple join keys in every downstream table |
| `filing_form_type_present` | reject | `form_type IS NOT NULL AND length(form_type) > 0` | Null form types making amendment detection and gold activity counts silently wrong |
| `filing_filed_date_present` | reject | `filed_date IS NOT NULL` | Null filed dates breaking restatement ordering, which is ordered by filed_date |
| `filing_filed_date_sane` | warn | `filed_date >= DATE'1993-01-01' AND filed_date <= current_date()` | Obviously wrong filed dates (pre-EDGAR or future) polluting activity trends unnoticed |

### §3.2 `edgar.silver.company` — SCD-2

Natural key: `cik`. Tracked columns: `name, sic, sic_description,
state_of_incorporation, fiscal_year_end, tickers, exchanges`.
**Array columns are sorted before hashing `_hash_diff`** — source ordering is not
stable and unsorted hashing generates a spurious version every day.

| Column | Type | Null | Notes |
|---|---|---|---|
| `cik` | STRING | no | natural key |
| `name` | STRING | no | |
| `sic` | STRING | yes | |
| `sic_description` | STRING | yes | |
| `state_of_incorporation` | STRING | yes | |
| `fiscal_year_end` | STRING | yes | `MMDD` as reported |
| `tickers` | ARRAY<STRING> | yes | sorted ascending |
| `exchanges` | ARRAY<STRING> | yes | sorted ascending |
| `_hash_diff` | STRING | no | sha256 of tracked columns, arrays sorted, `\|`-delimited |
| `valid_from` | DATE | no | |
| `valid_to` | DATE | yes | null = open version |
| `is_current` | BOOLEAN | no | |
| `_first_seen_ts` | TIMESTAMP | no | |
| `_last_seen_ts` | TIMESTAMP | no | |
| `_batch_id` | STRING | no | |

**DQ checks (silver.company):**

| name | severity | expression | prevents |
|---|---|---|---|
| `company_cik_padded` | reject | `cik RLIKE '^[0-9]{10}$'` | Unpadded CIKs splitting one company across multiple join keys in every downstream table |
| `company_name_present` | reject | `name IS NOT NULL AND length(name) > 0` | Nameless company rows rendering as blanks in the UI search panel |
| `company_one_current_per_cik` | reject_batch | `sum(CASE WHEN is_current THEN 1 ELSE 0 END) OVER (PARTITION BY cik) = 1` | Multiple current SCD-2 versions per cik fanning out every downstream join and doubling gold row counts |
| `company_valid_range` | reject_batch | `valid_to IS NULL OR valid_to >= valid_from` | Negative validity intervals corrupting as-of joins against the dimension |

### §3.3 `edgar.silver.financial_fact` — bitemporal

**Grain: `(cik, taxonomy, concept, unit, period_start, period_end,
accession_number)`.** The asserting accession is part of the grain — this is what
makes restatement detection a query instead of a reconciliation job. The same
`(cik, concept, period)` reported by two accessions is **two rows**.

| Column | Type | Null | Notes |
|---|---|---|---|
| `cik` | STRING | no | |
| `taxonomy` | STRING | no | `us-gaap` in MVP |
| `concept` | STRING | no | as filed |
| `concept_canonical` | STRING | yes | per §0.3, null when unmapped |
| `unit` | STRING | no | e.g. `USD`, `USD-per-shares` |
| `value` | DECIMAL(38,6) | no | |
| `period_start` | DATE | yes | null for instant facts |
| `period_end` | DATE | no | |
| `period_type` | STRING | no | `instant` \| `duration` |
| `fy` | STRING | yes | fiscal year label as reported |
| `fp` | STRING | yes | fiscal period label as reported |
| `form` | STRING | yes | form of the asserting filing |
| `accession_number` | STRING | no | asserting accession — part of the grain |
| `filed_date` | DATE | no | when the assertion was filed |
| `_first_seen_ts` | TIMESTAMP | no | |
| `_last_seen_ts` | TIMESTAMP | no | |
| `_batch_id` | STRING | no | |

**DQ checks (silver.financial_fact):**

| name | severity | expression | prevents |
|---|---|---|---|
| `fact_cik_padded` | reject | `cik RLIKE '^[0-9]{10}$'` | Unpadded CIKs splitting one company across multiple join keys in every downstream table |
| `fact_accession_format` | reject | `accession_number RLIKE '^[0-9]{10}-[0-9]{2}-[0-9]{6}$'` | Malformed asserting accessions breaking the restatement self-join and sec.gov links |
| `fact_unit_present` | reject | `unit IS NOT NULL AND length(unit) > 0` | Null units letting restatement detection compare values measured in different units |
| `fact_value_present` | reject | `value IS NOT NULL` | Null fact values crashing delta computation in gold restatement detection |
| `fact_period_end_present` | reject | `period_end IS NOT NULL` | Facts without a period end being unassignable to any reporting period in gold |
| `fact_period_order` | reject | `period_start IS NULL OR period_end >= period_start` | Negative-length duration periods poisoning period-scoped comparisons (instant facts pass by design) |
| `fact_grain_unique` | reject_batch | `count(*) OVER (PARTITION BY cik, taxonomy, concept, unit, period_start, period_end, accession_number) = 1` | Duplicate rows at the declared grain double-counting facts and fabricating restatement events |
| `fact_value_magnitude` | warn | `abs(value) < 1e15` | Unit-scale mistakes (dollars misread as millions) flooding gold with absurd values unnoticed |

### §3.4 Quarantine tables

`edgar.silver.filing_quarantine`, `edgar.silver.company_quarantine`,
`edgar.silver.financial_fact_quarantine`. Each carries the columns of its source table
**with every column nullable** (a quarantined row is by definition malformed), plus:

| Column | Type | Null |
|---|---|---|
| `_dq_check_name` | STRING | no |
| `_dq_failure_reason` | STRING | no |
| `_dq_run_id` | STRING | no |
| `_quarantined_at` | TIMESTAMP | no |

---

## §4 Gold

### §4.1 `edgar.gold.financials_current`

Latest assertion per `(cik, concept, unit, period_start, period_end)`, i.e. the
restated view of the world. One row per key, the assertion with the greatest
`filed_date` (ties broken by `accession_number` descending).

| Column | Type | Null |
|---|---|---|
| `cik` | STRING | no |
| `concept` | STRING | no |
| `concept_canonical` | STRING | yes |
| `unit` | STRING | no |
| `value` | DECIMAL(38,6) | no |
| `period_start` | DATE | yes |
| `period_end` | DATE | no |
| `period_type` | STRING | no |
| `fy` | STRING | yes |
| `fp` | STRING | yes |
| `accession_number` | STRING | no |
| `filed_date` | DATE | no |
| `is_restated` | BOOLEAN | no |
| `_updated_ts` | TIMESTAMP | no |

### §4.2 `edgar.gold.restatement_event`

Consecutive assertions of the same fact with a materially different value. Comparison
is scoped to identical `(cik, concept_canonical, unit, period_start, period_end,
period_type)`, ordered by `filed_date`, and uses **relative tolerance, never `!=`**:

```sql
abs(later.value - earlier.value) > greatest(abs(earlier.value) * 1e-6, 1e-6)
```

| Column | Type | Null | Notes |
|---|---|---|---|
| `event_id` | STRING | no | sha256 of the grain + both accessions |
| `cik` | STRING | no | |
| `concept` | STRING | no | as filed on the later assertion |
| `concept_canonical` | STRING | yes | |
| `unit` | STRING | no | |
| `period_start` | DATE | yes | |
| `period_end` | DATE | no | |
| `period_type` | STRING | no | |
| `original_accession` | STRING | no | |
| `original_filed_date` | DATE | no | |
| `original_value` | DECIMAL(38,6) | no | |
| `restated_accession` | STRING | no | |
| `restated_filed_date` | DATE | no | |
| `restated_value` | DECIMAL(38,6) | no | |
| `delta_value` | DECIMAL(38,6) | no | restated − original |
| `delta_pct` | DOUBLE | yes | **null when `original_value = 0`**, never an exception |
| `days_to_restatement` | INT | no | `restated_filed_date − original_filed_date` |
| `materiality_band` | STRING | no | `immaterial` <1%, `notable` 1–5%, `material` >5% — a **product heuristic, not an accounting standard** |
| `detected_ts` | TIMESTAMP | no | |

### §4.3 `edgar.gold.filing_activity_daily`

| Column | Type | Null |
|---|---|---|
| `filed_date` | DATE | no |
| `base_form_type` | STRING | no |
| `filing_count` | BIGINT | no |
| `amendment_count` | BIGINT | no |
| `distinct_ciks` | BIGINT | no |
| `_updated_ts` | TIMESTAMP | no |

### §4.4 `edgar.gold.company_profile`

One row per company in the universe.

| Column | Type | Null |
|---|---|---|
| `cik` | STRING | no |
| `name` | STRING | no |
| `tickers` | ARRAY<STRING> | yes |
| `exchanges` | ARRAY<STRING> | yes |
| `sic` | STRING | yes |
| `sic_description` | STRING | yes |
| `state_of_incorporation` | STRING | yes |
| `first_filing_date` | DATE | yes |
| `latest_filing_date` | DATE | yes |
| `filing_count` | BIGINT | no |
| `restatement_count` | BIGINT | no |
| `_updated_ts` | TIMESTAMP | no |

---

## §5 Serving export

Repo 4 exports each gold table to:

```
s3://{serving_bucket}/v1/{table}/data.parquet      (overwrite, never append)
s3://{serving_bucket}/v1/_manifest.json
```

`_manifest.json`:

```json
{
  "generated_at": "2026-07-30T06:41:02Z",
  "contracts_version": "1.0.0",
  "gold_max_filed_date": "2026-07-29",
  "row_counts": {
    "financials_current": 41250,
    "restatement_event": 312,
    "filing_activity_daily": 1180,
    "company_profile": 500
  }
}
```

Invariant: `gold_max_filed_date == max(filed_date) in edgar.silver.filing` at export
time. Repo 5's `/health` returns 503 when `now − gold_max_filed_date > 48h`.

---

## 6. Table reference (v1.0.0)

Every table, exactly as created by `changelog/050-realign-v1.yaml` and mirrored
by `spark/schemas.py`. The two are diffed on every CI run by the drift test.

**What changed from v0.1.0, and why:** the v0.1.0 shapes were written before any
consumer existed and disagreed with repo 4 on nearly every table. Metadata
columns were renamed to what the consumer reads (`_batch_id` →
`_ingest_batch_id`, `_schema_version` → `_envelope_version`, `_source_system`
added); bronze gained the provenance columns the landing envelope now carries
(`resource_id`, `fetched_at`, `logical_date`); `silver.financial_fact` gained
`decimals`, which is what makes a rounding-only restatement detectable rather
than a false positive; and quarantine moved from a per-table mirror to one
generic `record_json` shape so a new table needs no new quarantine DDL.

### `edgar.bronze.filing_index_raw`

| Column | Type | Null |
|---|---|---|
| `logical_date` | DATE | no |
| `resource_id` | STRING | yes |
| `fetched_at` | TIMESTAMP | yes |
| `form_type` | STRING | yes |
| `company_name` | STRING | yes |
| `cik` | STRING | yes |
| `date_filed` | STRING | yes |
| `accession_number` | STRING | yes |
| `file_name` | STRING | yes |
| `_ingest_batch_id` | STRING | no |
| `_ingest_ts` | TIMESTAMP | no |
| `_source_file` | STRING | no |
| `_source_system` | STRING | no |
| `_envelope_version` | STRING | no |
| `_rescued_data` | STRING | yes |

### `edgar.bronze.company_submissions_raw`

| Column | Type | Null |
|---|---|---|
| `logical_date` | DATE | no |
| `resource_id` | STRING | yes |
| `fetched_at` | TIMESTAMP | yes |
| `cik` | STRING | yes |
| `payload_json` | STRING | yes |
| `_ingest_batch_id` | STRING | no |
| `_ingest_ts` | TIMESTAMP | no |
| `_source_file` | STRING | no |
| `_source_system` | STRING | no |
| `_envelope_version` | STRING | no |
| `_rescued_data` | STRING | yes |

### `edgar.bronze.company_concept_raw`

| Column | Type | Null |
|---|---|---|
| `logical_date` | DATE | no |
| `resource_id` | STRING | yes |
| `fetched_at` | TIMESTAMP | yes |
| `cik` | STRING | yes |
| `taxonomy` | STRING | yes |
| `tag` | STRING | yes |
| `payload_json` | STRING | yes |
| `_ingest_batch_id` | STRING | no |
| `_ingest_ts` | TIMESTAMP | no |
| `_source_file` | STRING | no |
| `_source_system` | STRING | no |
| `_envelope_version` | STRING | no |
| `_rescued_data` | STRING | yes |

### `edgar.silver.filing`

| Column | Type | Null |
|---|---|---|
| `accession_number` | STRING | no |
| `cik` | STRING | no |
| `company_name` | STRING | yes |
| `form_type` | STRING | no |
| `base_form_type` | STRING | no |
| `is_amendment` | BOOLEAN | no |
| `filed_date` | DATE | no |
| `primary_doc_url` | STRING | yes |
| `logical_date` | DATE | no |
| `_first_seen_ts` | TIMESTAMP | no |
| `_last_seen_ts` | TIMESTAMP | no |
| `_ingest_batch_id` | STRING | no |
| `_source_file` | STRING | yes |

### `edgar.silver.company`

| Column | Type | Null |
|---|---|---|
| `cik` | STRING | no |
| `company_name` | STRING | yes |
| `sic` | STRING | yes |
| `sic_description` | STRING | yes |
| `ein` | STRING | yes |
| `entity_type` | STRING | yes |
| `state_of_incorporation` | STRING | yes |
| `fiscal_year_end` | STRING | yes |
| `tickers` | ARRAY<STRING> | yes |
| `exchanges` | ARRAY<STRING> | yes |
| `former_names` | ARRAY<STRING> | yes |
| `valid_from` | DATE | no |
| `valid_to` | DATE | yes |
| `is_current` | BOOLEAN | no |
| `_hash_diff` | STRING | no |
| `_first_seen_ts` | TIMESTAMP | no |
| `_last_seen_ts` | TIMESTAMP | no |
| `_ingest_batch_id` | STRING | no |
| `_source_file` | STRING | yes |

### `edgar.silver.financial_fact`

| Column | Type | Null |
|---|---|---|
| `cik` | STRING | no |
| `taxonomy` | STRING | no |
| `concept_tag` | STRING | no |
| `concept_canonical` | STRING | yes |
| `unit` | STRING | no |
| `period_start` | DATE | yes |
| `period_end` | DATE | no |
| `period_type` | STRING | no |
| `accession_number` | STRING | no |
| `value` | DECIMAL(38,6) | yes |
| `decimals` | INT | yes |
| `fiscal_year` | INT | yes |
| `fiscal_period` | STRING | yes |
| `form_type` | STRING | yes |
| `filed_date` | DATE | no |
| `frame` | STRING | yes |
| `logical_date` | DATE | no |
| `_first_seen_ts` | TIMESTAMP | no |
| `_last_seen_ts` | TIMESTAMP | no |
| `_ingest_batch_id` | STRING | no |
| `_source_file` | STRING | yes |

### `edgar.silver.filing_quarantine`

| Column | Type | Null |
|---|---|---|
| `_dq_record_id` | STRING | no |
| `_dq_run_id` | STRING | no |
| `_dq_check_name` | STRING | no |
| `_dq_failure_reason` | STRING | no |
| `_quarantined_at` | TIMESTAMP | no |
| `_source_table` | STRING | no |
| `_source_file` | STRING | yes |
| `_ingest_batch_id` | STRING | yes |
| `record_json` | STRING | no |

### `edgar.silver.company_quarantine`

| Column | Type | Null |
|---|---|---|
| `_dq_record_id` | STRING | no |
| `_dq_run_id` | STRING | no |
| `_dq_check_name` | STRING | no |
| `_dq_failure_reason` | STRING | no |
| `_quarantined_at` | TIMESTAMP | no |
| `_source_table` | STRING | no |
| `_source_file` | STRING | yes |
| `_ingest_batch_id` | STRING | yes |
| `record_json` | STRING | no |

### `edgar.silver.financial_fact_quarantine`

| Column | Type | Null |
|---|---|---|
| `_dq_record_id` | STRING | no |
| `_dq_run_id` | STRING | no |
| `_dq_check_name` | STRING | no |
| `_dq_failure_reason` | STRING | no |
| `_quarantined_at` | TIMESTAMP | no |
| `_source_table` | STRING | no |
| `_source_file` | STRING | yes |
| `_ingest_batch_id` | STRING | yes |
| `record_json` | STRING | no |

### `edgar.gold.financials_current`

| Column | Type | Null |
|---|---|---|
| `cik` | STRING | no |
| `company_name` | STRING | yes |
| `concept_canonical` | STRING | no |
| `unit` | STRING | no |
| `period_start` | DATE | yes |
| `period_end` | DATE | no |
| `period_type` | STRING | no |
| `value` | DECIMAL(38,6) | yes |
| `decimals` | INT | yes |
| `fiscal_year` | INT | yes |
| `fiscal_period` | STRING | yes |
| `accession_number` | STRING | no |
| `form_type` | STRING | yes |
| `filed_date` | DATE | no |
| `assertion_count` | INT | no |
| `was_restated` | BOOLEAN | no |
| `_generated_at` | TIMESTAMP | no |
| `_run_id` | STRING | no |

### `edgar.gold.restatement_event`

| Column | Type | Null |
|---|---|---|
| `restatement_id` | STRING | no |
| `cik` | STRING | no |
| `company_name` | STRING | yes |
| `concept_canonical` | STRING | no |
| `unit` | STRING | no |
| `period_start` | DATE | yes |
| `period_end` | DATE | no |
| `period_type` | STRING | no |
| `original_accession_number` | STRING | no |
| `original_form_type` | STRING | yes |
| `original_filed_date` | DATE | no |
| `original_value` | DECIMAL(38,6) | no |
| `original_decimals` | INT | yes |
| `restated_accession_number` | STRING | no |
| `restated_form_type` | STRING | yes |
| `restated_filed_date` | DATE | no |
| `restated_value` | DECIMAL(38,6) | no |
| `restated_decimals` | INT | yes |
| `delta_abs` | DECIMAL(38,6) | no |
| `delta_pct` | DOUBLE | yes |
| `materiality_band` | STRING | no |
| `days_to_restatement` | INT | no |
| `_generated_at` | TIMESTAMP | no |
| `_run_id` | STRING | no |

### `edgar.gold.filing_activity_daily`

| Column | Type | Null |
|---|---|---|
| `filed_date` | DATE | no |
| `base_form_type` | STRING | no |
| `filing_count` | INT | no |
| `amendment_count` | INT | no |
| `distinct_cik_count` | INT | no |
| `_generated_at` | TIMESTAMP | no |
| `_run_id` | STRING | no |

### `edgar.gold.company_profile`

| Column | Type | Null |
|---|---|---|
| `cik` | STRING | no |
| `company_name` | STRING | yes |
| `sic` | STRING | yes |
| `sic_description` | STRING | yes |
| `entity_type` | STRING | yes |
| `state_of_incorporation` | STRING | yes |
| `fiscal_year_end` | STRING | yes |
| `tickers` | ARRAY<STRING> | yes |
| `exchanges` | ARRAY<STRING> | yes |
| `filing_count` | INT | no |
| `first_filed_date` | DATE | yes |
| `last_filed_date` | DATE | yes |
| `restatement_count` | INT | no |
| `_generated_at` | TIMESTAMP | no |
| `_run_id` | STRING | no |
