# BoF Data — Finnish Loan Data Explorer

A demo project for exploring Finnish loan data (Bank of Finland / Suomen Pankki) using Databricks.

## Purpose

This project provides version-controlled Databricks pipelines and jobs for ingesting,
transforming, and analysing Finnish loan data. The goal is to demonstrate a reproducible,
code-first approach to data engineering and exploratory analysis on Databricks.

## What's included

- **Pipelines** — Delta Live Tables (DLT) or Databricks Workflows definitions for data ingestion and transformation
- **Jobs** — Databricks job configurations (YAML/JSON) tracked in version control
- **Notebooks** — Exploratory analysis and visualisation notebooks

## Data source

Finnish loan statistics published by the Bank of Finland (Suomen Pankki / BoF).

## Getting started

1. Clone this repository
2. Configure your Databricks workspace credentials (`.env` or Databricks CLI profile)
3. Deploy pipelines and jobs using the Databricks CLI:
   ```bash
   databricks bundle deploy
   ```
4. Trigger a run:
   ```bash
   databricks bundle run <job-name>
   ```

## Project structure

```
.
├── databricks.yml       # Databricks Asset Bundle root config
├── jobs/                # Job definitions
├── pipelines/           # DLT pipeline definitions
├── notebooks/           # Analysis notebooks
└── src/                 # Shared Python source code
```

## Requirements

- Python 3.10+
- Databricks CLI v0.200+ (Asset Bundles support)
- Access to a Databricks workspace
