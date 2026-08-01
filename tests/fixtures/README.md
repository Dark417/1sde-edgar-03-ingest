# Test fixtures — provenance

Real EDGAR responses, fetched by hand and trimmed. **No test may hit the network**
(`AGENTS.md` §5.11); everything here is served through `respx`.

All were fetched on 2026-08-01 with
`User-Agent: edgar-lakehouse-demo dark.show.time@gmail.com`.

| File | Source | Trimming |
|---|---|---|
| `form.20260729.idx` | `www.sec.gov/Archives/edgar/daily-index/2026/QTR3/form.20260729.idx` | header verbatim (11 lines) + 47 of 5,969 data rows |
| `submissions_CIK0000320193.json` | `data.sec.gov/submissions/CIK0000320193.json` | `filings.recent.*` arrays cut to 5 entries, `filings.files` to 1; all 24 top-level keys kept |
| `companyconcept_CIK0000320193_Assets.json` | `data.sec.gov/api/xbrl/companyconcept/CIK0000320193/us-gaap/Assets.json` | `units.USD` cut to 6 entries |
| `companyconcept_CIK0000320193_Revenues.json` | `.../us-gaap/Revenues.json` | `units.USD` cut to 6 entries |
| `companyconcept_404_body.xml` | `.../us-gaap/CostOfRevenue.json` | verbatim, untrimmed (330 bytes) |

## `form.20260729.idx` — why these 47 rows

The file is sorted by form type, so a contiguous `head -60` yields only three distinct
form types and tests nothing. The 47 rows are a stride sample across all 5,969 rows,
giving **26 distinct form types**, plus four rows selected explicitly because they are
the ones that break a fixed-width parser:

- the widest `form_type` (15 chars — including `SEC STAFF ACTION`, which **contains a
  space**, so a whitespace-splitting parser produces garbage for it)
- the longest `company_name` (60 chars, near the 62-char field width)
- the longest `cik`
- the longest `file_name`

The header block is preserved byte-for-byte, including EDGAR's hard wrap of the
column-name line across two physical lines.

## `companyconcept_404_body.xml` — why it is XML

Apple **genuinely does not report** `us-gaap:CostOfRevenue`; that URL 404s today. This
is the real-world case behind `AGENTS.md` §5.4 (a 404 from `company_concept` is not an
error).

The body is XML (an S3 `NoSuchKey` error), not JSON, despite the `.json` URL. The
client must therefore decide on the **status code** and never attempt to parse a 404
body.

## Refreshing

Fixtures are refreshed by hand, not by a script, and the result is eyeballed before
committing — the fixed-width layout has changed before and will change again, and the
whole point of these files is to be the thing that notices.
