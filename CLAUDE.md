# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this project does

Databricks Asset Bundle that ingests Finnish loan statistics from the [Bank of Finland FSA Open Data API v4](https://api.boffsaopendata.fi/v4) into Delta tables using a two-layer medallion architecture. Both jobs are manually triggered — no schedule.

## Deployment commands

```bash
# Deploy to dev (default target)
databricks bundle deploy

# Deploy to prod
databricks bundle deploy --target prod

# Run the bronze ingest job
databricks bundle run ingest_bof_loans

# Run with overrides (e.g. incremental load)
databricks bundle run ingest_bof_loans \
  --param dataset_id=MFI_PUBL \
  --param start_period=2024-01 \
  --param end_period=2024-12

# Run the silver transform job (after bronze)
databricks bundle run silver_bof_loans

# Validate bundle config without deploying
databricks bundle validate
```

The workspace host is supplied via the `workspace_host` variable — set it in a `databricks.yml` override, environment variable, or `.databricks/` profile.

## Architecture

```
BoF FSA Open Data API v4
  └─ GET /v4/Observations/{dataset_id}?pageSize=500&pageNumber=N
       │  paginated: items[].name + items[].observations[]{period,periodCode,value}
       ▼
src/ingest_bof_loans.py          (bronze job)
  ├─ fetch_observations()  — pages through all series, flattens to row-per-observation
  ├─ resolve_catalog()     — uses --catalog arg or auto-detects first writable catalog
  ├─ ensure_table()        — CREATE TABLE IF NOT EXISTS with CDF enabled
  └─ DeltaTable.merge()    — MERGE ON (dataset_id, series_name, period); insert-only
       ▼
{catalog}.bof.loan_observations          (Bronze — raw, string period, opaque series_name)
       ▼
src/silver_bof_loans.py          (silver job)
  ├─ build_silver_df()     — splits series_name on "." into 18 dim_* columns,
  │                          casts period → DATE, drops period_code, filters nulls
  └─ DeltaTable.merge()    — MERGE ON (dataset_id, series_name, period_date);
                             update-on-match so revised values propagate
       ▼
{catalog}.bof_silver.loan_observations   (Silver — typed, queryable by dimension)
```

**Bronze dedup key**: `(dataset_id, series_name, period)` — insert-only MERGE, won't overwrite changed values.

**Silver dedup key**: `(dataset_id, series_name, period_date)` — update-on-match so revisions from bronze flow through.

**Series name dimensions** (MFI_PUBL — 18 dot-separated SDMX codes, mapped to `dim_*` columns):

| pos | column | example | meaning |
|-----|--------|---------|---------|
| 1 | `dim_freq` | `M` | Monthly |
| 2 | `dim_reporter_group` | `A` | MFIs excl. BoF |
| 3 | `dim_measure` | `0`/`1` | Volume / Rate |
| 4 | `dim_transaction_type` | `A`/`N` | Stock / New drawdown |
| 5 | `dim_bs_item` | `A20`/`L20` | Loans / Deposits |
| 6 | `dim_maturity_orig` | | Original maturity bucket |
| 7 | `dim_maturity_rem` | | Remaining maturity bucket |
| 8 | `dim_count_area` | `U6` | Domestic |
| 9 | `dim_count_sector` | `22C0` | Counterpart sector |
| 10 | `dim_count_industry` | `ZZ` | Counterpart industry |
| 11 | `dim_currency` | `Z01` | All currencies combined |
| 12–18 | `dim_loan_purpose` … `dim_notice_type` | | Loan sub-classifications |

**Catalog resolution**: both scripts auto-detect the first writable catalog if `--catalog` is empty. Pass it explicitly in prod.

## Job parameters

**ingest_bof_loans** (bronze):

| Parameter      | Default    | Notes                                  |
|---------------|------------|----------------------------------------|
| `dataset_id`  | `MFI_PUBL` | BoF API dataset identifier             |
| `start_period`| `""`       | Empty = full history                   |
| `end_period`  | `""`       | Empty = latest available               |
| `catalog`     | `workspace`| Unity Catalog catalog name             |
| `schema`      | `bof`      | Bronze schema                          |

**silver_bof_loans** (silver):

| Parameter    | Default      | Notes                        |
|-------------|--------------|------------------------------|
| `catalog`   | `workspace`  | Unity Catalog catalog name   |
| `src_schema`| `bof`        | Bronze schema to read from   |
| `dst_schema`| `bof_silver` | Silver schema to write into  |

## Key files

- `databricks.yml` — bundle root; defines `dev`/`prod` targets and the `workspace_host` variable
- `jobs/ingest_bof_loans.yml` — bronze job definition
- `jobs/silver_bof_loans.yml` — silver job definition
- `src/ingest_bof_loans.py` — bronze ingestion: API fetch → Delta MERGE
- `src/silver_bof_loans.py` — silver transform: dimension parsing, type casting → Delta MERGE
