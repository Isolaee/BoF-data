"""
Ingest Bank of Finland loan observations from the BoF FSA Open Data API (v4)
into a Delta table. Uses MERGE to ensure idempotency — safe to re-run.

Dedup key: (dataset_id, series_name, period)
"""

import argparse
import requests
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)
from delta.tables import DeltaTable

BASE_URL = "https://api.boffsaopendata.fi/v4"
PAGE_SIZE = 10_000  # observations per request


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--dataset_id", default="MFI_PUBL")
    p.add_argument("--start_period", default="")
    p.add_argument("--end_period", default="")
    p.add_argument("--catalog", default="")
    p.add_argument("--schema", default="bof")
    return p.parse_args()


def fetch_observations(dataset_id: str, start_period: str, end_period: str) -> list[dict]:
    """Page through the Observations endpoint and return all records."""
    params = {"pageSize": PAGE_SIZE, "pageNumber": 1}
    if start_period:
        params["startPeriod"] = start_period
    if end_period:
        params["endPeriod"] = end_period

    records = []
    while True:
        resp = requests.get(
            f"{BASE_URL}/Observations/{dataset_id}",
            params=params,
            timeout=60,
        )
        resp.raise_for_status()
        data = resp.json()

        batch = data.get("observations", [])
        records.extend(batch)

        # Stop when we've received fewer records than the page size
        if len(batch) < PAGE_SIZE:
            break
        params["pageNumber"] += 1

    return records


SCHEMA = StructType([
    StructField("dataset_id",   StringType(),    nullable=False),
    StructField("series_name",  StringType(),    nullable=False),
    StructField("period",       StringType(),    nullable=False),
    StructField("value",        DoubleType(),    nullable=True),
    StructField("unit",         StringType(),    nullable=True),
    StructField("status",       StringType(),    nullable=True),
    StructField("ingested_at",  TimestampType(), nullable=False),
])

MERGE_CONDITION = (
    "target.dataset_id  = source.dataset_id  AND "
    "target.series_name = source.series_name AND "
    "target.period      = source.period"
)


def to_rows(dataset_id: str, observations: list[dict], ingested_at) -> list[dict]:
    return [
        {
            "dataset_id":  dataset_id,
            "series_name": obs.get("seriesName", ""),
            "period":      obs.get("period", ""),
            "value":       obs.get("value"),
            "unit":        obs.get("unit"),
            "status":      obs.get("observationStatus"),
            "ingested_at": ingested_at,
        }
        for obs in observations
    ]


def ensure_table(spark: SparkSession, full_table: str):
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {full_table} (
            dataset_id  STRING  NOT NULL,
            series_name STRING  NOT NULL,
            period      STRING  NOT NULL,
            value       DOUBLE,
            unit        STRING,
            status      STRING,
            ingested_at TIMESTAMP NOT NULL
        )
        USING DELTA
        PARTITIONED BY (dataset_id)
        TBLPROPERTIES (
            'delta.enableChangeDataFeed' = 'true',
            'delta.minReaderVersion'     = '1',
            'delta.minWriterVersion'     = '2'
        )
    """)


def main():
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()

    catalog = args.catalog if args.catalog else spark.catalog.currentCatalog()
    print(f"Using catalog: {catalog}")

    full_table = f"{catalog}.{args.schema}.loan_observations"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{args.schema}")
    ensure_table(spark, full_table)

    print(f"Fetching observations: dataset={args.dataset_id} "
          f"start={args.start_period or 'all'} end={args.end_period or 'latest'}")

    raw = fetch_observations(args.dataset_id, args.start_period, args.end_period)
    print(f"Fetched {len(raw):,} observations")

    if not raw:
        print("No observations returned — nothing to write.")
        return

    ingested_at = datetime.now(timezone.utc)
    rows = to_rows(args.dataset_id, raw, ingested_at)

    df = spark.createDataFrame(rows, schema=SCHEMA)

    # MERGE — insert only new (dataset_id, series_name, period) combinations
    DeltaTable.forName(spark, full_table) \
        .alias("target") \
        .merge(df.alias("source"), MERGE_CONDITION) \
        .whenNotMatchedInsertAll() \
        .execute()

    inserted = spark.sql(
        f"SELECT COUNT(*) FROM {full_table} WHERE dataset_id = '{args.dataset_id}'"
    ).collect()[0][0]
    print(f"Done. Total rows in table for {args.dataset_id}: {inserted:,}")


if __name__ == "__main__":
    main()
