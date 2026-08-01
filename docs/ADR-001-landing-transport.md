# ADR-001 — Landing transport: S3 vs Databricks Volume

**Status: UNRESOLVED** — fill in after running the runbook step 0 probe.
Until resolved, all code generates against `LANDING_MODE=volume` (the safe
default: it never assumes cloud-credential passthrough exists).

## Context

Ingest (repo 3) writes every batch to two sinks: S3 `edgar-lake-raw` (system of
record, commits first) and a Databricks-visible landing location that Auto
Loader (repo 4) reads. The question this ADR answers: **which location does
Auto Loader read from?**

Databricks Free Edition does not guarantee cloud-credential passthrough — the
workspace may not be able to read `s3://edgar-lake-raw` directly. The probe
determines whether it can.

## Probe (runbook step 0)

In a Free Edition notebook, after repo 2 has created the catalog and volume:

```python
# 1. Can the workspace list the raw bucket directly?
dbutils.fs.ls("s3://edgar-lake-raw/")  # works -> s3 mode is possible

# 2. Can the workspace read the managed volume? (expected: always yes)
dbutils.fs.ls("/Volumes/edgar/landing/edgar/")
```

## Decision table

| Probe result | `LANDING_MODE` | Consequence |
|---|---|---|
| S3 readable from the workspace | `s3` | Auto Loader reads `s3://edgar-lake-raw/edgar/...`; the Volume sink can be disabled (one path deletes itself) |
| S3 not readable | `volume` | Auto Loader reads `/Volumes/edgar/landing/edgar/...`; ingest keeps the dual sink; S3 remains the system of record and replay source |

## Decision

_To be filled by hand with the probe result and date._

- `LANDING_MODE = ________` (probed on ________)

## Consequences

- The value is published to SSM as `/edgar-lakehouse/landing_mode` (repo 2) and
  consumed by repos 3 and 4 at runtime.
- Both sinks write byte-identical payloads to the same filename regardless of
  mode, so switching modes later does not invalidate history.
