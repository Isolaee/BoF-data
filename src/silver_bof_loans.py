"""
Transform bronze loan_observations into silver:
  - Split dot-separated series_name into 18 named SDMX dimension columns
  - Cast period string (YYYY-MM-DD) to DATE
  - Drop redundant period_code
  - MERGE into silver table (update on match so revised values propagate)

Dedup key: (dataset_id, series_name, period_date)
"""

import argparse
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, DateType, TimestampType,
)
from delta.tables import DeltaTable


# MFI_PUBL DSD — 18 positional SDMX dimensions in series_name
DIMS = [
    "freq",             # pos  1 — e.g. M (Monthly)
    "reporter_group",   # pos  2 — e.g. A (MFIs excl. BoF)
    "measure",          # pos  3 — e.g. 0 (Volume) / 1 (Rate)
    "transaction_type", # pos  4 — e.g. A (Stock) / N (New drawdown)
    "bs_item",          # pos  5 — e.g. A20 (Loans), L20 (Deposits)
    "maturity_orig",    # pos  6
    "maturity_rem",     # pos  7
    "count_area",       # pos  8 — e.g. U6 (Domestic)
    "count_sector",     # pos  9 — e.g. 22C0 (NFC+households)
    "count_industry",   # pos 10
    "currency",         # pos 11 — e.g. Z01 (All currencies)
    "loan_purpose",     # pos 12
    "collateral_type",  # pos 13
    "loan_size",        # pos 14
    "int_rate_link",    # pos 15
    "init_fix_period",  # pos 16
    "int_reset_type",   # pos 17
    "notice_type",      # pos 18
]

SILVER_SCHEMA = StructType(
    [
        StructField("dataset_id",      StringType(),    nullable=False),
        StructField("series_name",     StringType(),    nullable=False),
        StructField("period_date",     DateType(),      nullable=False),
        StructField("value",           DoubleType(),    nullable=True),
        StructField("silver_updated_at", TimestampType(), nullable=False),
    ]
    + [StructField(f"dim_{d}", StringType(), nullable=True) for d in DIMS]
)

MERGE_CONDITION = (
    "t.dataset_id = s.dataset_id AND "
    "t.series_name = s.series_name AND "
    "t.period_date = s.period_date"
)


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--catalog",    default="",    help="Unity Catalog name (auto-detected if empty)")
    p.add_argument("--src_schema", default="bof", help="Bronze schema")
    p.add_argument("--dst_schema", default="bof_silver", help="Silver schema")
    return p.parse_args()


def resolve_catalog(spark: SparkSession, catalog_arg: str) -> str:
    if catalog_arg:
        return catalog_arg
    _readonly = {"system", "__databricks_internal", "spark_catalog", "samples"}
    all_catalogs = [r.catalog for r in spark.sql("SHOW CATALOGS").collect()]
    available = [c for c in all_catalogs if c not in _readonly]
    if not available:
        raise RuntimeError("No writable catalogs found. Pass --catalog explicitly.")
    return available[0]


def ensure_silver_table(spark: SparkSession, full_table: str):
    dim_cols = "\n".join(f"    dim_{d:<20} STRING," for d in DIMS)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {full_table} (
            dataset_id           STRING    NOT NULL,
            series_name          STRING    NOT NULL,
            period_date          DATE      NOT NULL,
            value                DOUBLE,
            silver_updated_at    TIMESTAMP NOT NULL,
{dim_cols}
        )
        USING DELTA
        PARTITIONED BY (dataset_id)
        TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
    """)


def build_silver_df(spark: SparkSession, bronze_table: str, updated_at):
    bronze = spark.table(bronze_table)

    # Split series_name into an array then extract each dimension by index
    split_col = F.split(F.col("series_name"), r"\.")
    dim_selects = [
        F.split(F.col("series_name"), r"\.")[i].alias(f"dim_{name}")
        for i, name in enumerate(DIMS)
    ]

    return (
        bronze
        .filter(F.col("value").isNotNull())
        .select(
            F.col("dataset_id"),
            F.col("series_name"),
            F.to_date(F.col("period")).alias("period_date"),
            F.col("value"),
            F.lit(updated_at).alias("silver_updated_at"),
            *dim_selects,
        )
    )


def main():
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()

    catalog    = resolve_catalog(spark, args.catalog)
    bronze_tbl = f"{catalog}.{args.src_schema}.loan_observations"
    silver_tbl = f"{catalog}.{args.dst_schema}.loan_observations"

    print(f"Source (bronze): {bronze_tbl}")
    print(f"Target (silver): {silver_tbl}")

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{args.dst_schema}")
    ensure_silver_table(spark, silver_tbl)

    updated_at = datetime.now(timezone.utc)
    silver_df = build_silver_df(spark, bronze_tbl, updated_at)

    update_map = {f"t.{c.name}": f"s.{c.name}" for c in SILVER_SCHEMA}
    insert_map = update_map

    (
        DeltaTable.forName(spark, silver_tbl)
        .alias("t")
        .merge(silver_df.alias("s"), MERGE_CONDITION)
        .whenMatchedUpdate(set=update_map)
        .whenNotMatchedInsert(values=insert_map)
        .execute()
    )

    total = spark.sql(
        f"SELECT COUNT(*) FROM {silver_tbl} WHERE dataset_id = 'MFI_PUBL'"
    ).collect()[0][0]
    print(f"Done. Total silver rows for MFI_PUBL: {total:,}")


if __name__ == "__main__":
    main()
