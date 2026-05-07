# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Supplier FG Inventory vs IWL Daily Summary Fact
# MAGIC
# MAGIC Materialises the logic previously handled by `rpm.supplier_fg_inv_vs_iwl_daily_summary_vw`
# MAGIC into a partitioned Delta / Parquet fact table consumed by the RPMA AAS model.
# MAGIC
# MAGIC **Sources**
# MAGIC - `supplier_fg_inventory_fact` — finished-goods inventory snapshots
# MAGIC - `iwl_daily_fact` — in-warehouse-location daily stock
# MAGIC
# MAGIC **Target**: `rpm.supplier_fg_inv_vs_iwl_daily_summary_fact`
# MAGIC
# MAGIC **Schedule**: every 2 hours (Databricks job)
# MAGIC
# MAGIC **Strategy**
# MAGIC - First run → full load of the last 3 months
# MAGIC - Subsequent runs → recompute the last `BACKFILL_DAYS` snapshot dates and merge
# MAGIC   into temp (Delta), then dynamic-partition-overwrite into SA (Parquet)
# MAGIC - Sliding 3-month retention: records older than `LOOKBACK_MONTHS` are pruned from temp
# MAGIC   once per calendar day

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from delta.tables import DeltaTable
from datetime import date, timedelta

# ---------------------------------------------------------------------------
# Source tables  (replace placeholders with fully-qualified catalog.schema.table)
# ---------------------------------------------------------------------------
FG_SOURCE_TABLE  = "<catalog>.<schema>.supplier_fg_inventory_fact"   # PLACEHOLDER
IWL_SOURCE_TABLE = "<catalog>.<schema>.iwl_daily_fact"               # PLACEHOLDER

# ---------------------------------------------------------------------------
# Target
# ---------------------------------------------------------------------------
TARGET_TABLE_NAME = "supplier_fg_inv_vs_iwl_daily_summary_fact"
TARGET_PATH       = "/mnt/mda-pipeline-refined/supplier_fg_inv_vs_iwl_daily_summary_fact"
TEMP_PATH         = "/mnt/mda-pipeline-temp/supplier_fg_inv_vs_iwl_daily_summary_fact"

# ---------------------------------------------------------------------------
# Window / retention
# ---------------------------------------------------------------------------
LOOKBACK_MONTHS = 3   # sliding retention window that mirrors the original view
BACKFILL_DAYS   = 2   # how many trailing snapshot dates to recompute on each delta run

# ---------------------------------------------------------------------------
# Business key (used for DQ uniqueness check)
# stock_type_desc is part of the key because one (vendor, material, plant, date, BU)
# can have both "Inventory On-Ground" and "Inventory In-Transit" fg rows.
# NULL stock_type_desc indicates IWL-only rows.
# ---------------------------------------------------------------------------
BUSINESS_KEY = [
    "snapshot_date",
    "purchase_vendor_id",
    "material_id",
    "plant_code",
    "business_unit_lkp_code",
    "stock_type_desc",
]

PARTITION_COL = "snapshot_date"
STOCK_TYPES   = ["Inventory On-Ground", "Inventory In-Transit"]

# ---------------------------------------------------------------------------
# Date boundaries (computed once per run)
# ---------------------------------------------------------------------------
today          = date.today()
cutoff_date    = today - timedelta(days=LOOKBACK_MONTHS * 30)   # approximate 3-month floor
backfill_start = today - timedelta(days=BACKFILL_DAYS - 1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper: check whether temp table exists

# COMMAND ----------

def path_has_data(path: str) -> bool:
    try:
        files = dbutils.fs.ls(path)
        return any(f.name.endswith(".parquet") or "_delta_log" in f.name for f in files)
    except Exception:
        return False

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Read source data

# COMMAND ----------

# Predicate pushdown: limit source scans to the 3-month window up front
df_fg_raw = (
    spark.read.table(FG_SOURCE_TABLE)
    .filter(F.col("snapshot_date") >= F.lit(cutoff_date))
)

df_iwl_raw = (
    spark.read.table(IWL_SOURCE_TABLE)
    .filter(
        (F.col("purchase_vendor_id").isNotNull())
        & (F.col("calendar_date") >= F.lit(cutoff_date))
    )
)

print(f"FG raw rows (3M window)  : {df_fg_raw.count()}")
print(f"IWL raw rows (3M window) : {df_iwl_raw.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Build CTE equivalents

# COMMAND ----------

def build_fg_suppliers(df: DataFrame) -> DataFrame:
    """DISTINCT (vendor, plant, BU) — no date filter, mirrors the original view."""
    return df.select(
        "purchase_vendor_id",
        "business_unit_lkp_code",
        "plant_code",
    ).distinct()


def build_fg(df: DataFrame) -> DataFrame:
    """
    Aggregate FG inventory: On-Ground + In-Transit, last 3 months.
    Replicates the `fg` CTE from the original view.
    """
    return (
        df.filter(F.col("stock_type_desc").isin(STOCK_TYPES))
        .withColumn("snapshot_date", F.col("snapshot_date").cast("date"))
        .groupBy(
            "purchase_vendor_id",
            "material_id",
            "plant_code",
            "snapshot_date",
            "stock_type_desc",
            "business_unit_lkp_code",
        )
        .agg(F.sum("quantity").alias("quantity_ivy"))
        .filter(F.col("quantity_ivy") > 0)
    )


def build_iwl(df: DataFrame) -> DataFrame:
    """
    Aggregate IWL daily stock, last 3 months.
    Replicates the `iwl` CTE from the original view.
    Renames columns to match fg naming so the subsequent join uses a simple list.
    """
    return (
        df.groupBy(
            "purchase_vendor_id",
            F.col("material").alias("material_id"),
            F.col("plant").alias("plant_code"),
            F.col("calendar_date").alias("snapshot_date"),
            F.col("tdc_val").alias("business_unit_lkp_code"),
        )
        .agg(F.sum("buom_total_plant_stock").alias("quantity_iwl"))
        .filter(F.col("quantity_iwl") > 0)
    )


fg_suppliers = build_fg_suppliers(df_fg_raw)
df_fg        = build_fg(df_fg_raw)
df_iwl       = build_iwl(df_iwl_raw)

print(f"fg_suppliers distinct keys : {fg_suppliers.count()}")
print(f"fg aggregated rows         : {df_fg.count()}")
print(f"iwl aggregated rows        : {df_iwl.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Assemble the result

# COMMAND ----------

# Semi-join: keep only IWL rows whose (vendor, plant, BU) exists in FG.
# Equivalent to: fg_suppliers INNER JOIN iwl ON (vendor, plant, BU)
# Uses broadcast because fg_suppliers is a small distinct-key table.
join_keys_semi = ["purchase_vendor_id", "plant_code", "business_unit_lkp_code"]

iwl_filtered = df_iwl.join(
    F.broadcast(fg_suppliers),
    on=join_keys_semi,
    how="left_semi",
)

# Full outer join fg and filtered iwl on the complete business key (minus stock_type_desc).
# After an equi-join on shared column names Spark automatically coalesces the join keys,
# so we get a single set of key columns without COALESCE expressions.
join_keys_full = [
    "purchase_vendor_id",
    "material_id",
    "plant_code",
    "snapshot_date",
    "business_unit_lkp_code",
]

df_result = (
    iwl_filtered.join(df_fg, on=join_keys_full, how="full_outer")
    .select(
        "purchase_vendor_id",
        "material_id",
        "plant_code",
        "snapshot_date",
        "business_unit_lkp_code",
        "stock_type_desc",      # NULL for IWL-only rows
        "quantity_ivy",         # NULL for IWL-only rows
        "quantity_iwl",         # NULL for FG-only rows
        F.current_timestamp().alias("etl_load_ts"),
    )
    .repartition(F.col(PARTITION_COL))
)

print(f"Result rows (full window) : {df_result.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Determine load type and build delta

# COMMAND ----------

is_initial_load = not path_has_data(TEMP_PATH)

if is_initial_load:
    print("INITIAL LOAD — writing all 3-month data to temp")
    df_to_save   = df_result
    temp_save_mode = "overwrite"
else:
    print(f"DELTA LOAD — recomputing snapshot dates >= {backfill_start}")
    df_to_save   = df_result.filter(F.col(PARTITION_COL) >= F.lit(backfill_start))
    temp_save_mode = "overwrite"   # replaceWhere inside saveTabletemp (see Step 5)

new_count = df_to_save.count()
print(f"Rows to write: {new_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Save to temp (Delta, partition overwrite)

# COMMAND ----------

if new_count > 0:
    if is_initial_load:
        saveTabletemp(TARGET_TABLE_NAME, "delta", "overwrite", df_to_save)
    else:
        # Overwrite only the backfill partitions so we don't lose older history.
        # If the saveTabletemp helper does not support replaceWhere, fall back to
        # a direct Delta write with the replaceWhere option.
        try:
            (
                df_to_save.write
                .format("delta")
                .option("replaceWhere", f"{PARTITION_COL} >= '{backfill_start}'")
                .mode("overwrite")
                .save(TEMP_PATH)
            )
            print(f"Temp Delta written (replaceWhere >= {backfill_start})")
        except Exception as e:
            print(f"replaceWhere failed ({e}), falling back to saveTabletemp append")
            saveTabletemp(TARGET_TABLE_NAME, "delta", "append", df_to_save)
else:
    print("No new records — skipping temp write")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Sliding 3-month retention (run once per calendar day)

# COMMAND ----------

if path_has_data(TEMP_PATH):
    df_temp_check = spark.read.format("delta").load(TEMP_PATH)
    min_date = df_temp_check.agg(F.min(PARTITION_COL)).collect()[0][0]

    if min_date is not None and min_date < cutoff_date:
        print(f"Pruning records older than {cutoff_date} (current min: {min_date})")
        (
            DeltaTable.forPath(spark, TEMP_PATH)
            .delete(F.col(PARTITION_COL) < F.lit(cutoff_date))
        )
        print("Retention prune complete")
    else:
        print(f"Retention OK — oldest snapshot date: {min_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: DQ check

# COMMAND ----------

df_temp = spark.read.format("delta").load(TEMP_PATH)

# stock_type_desc can be NULL (IWL-only rows); fill a sentinel value so the
# uniqueness check treats NULLs as equal rather than always-distinct.
df_temp_dq = df_temp.withColumn(
    "stock_type_desc",
    F.coalesce(F.col("stock_type_desc"), F.lit("__IWL_ONLY__")),
)

result = expect_table_records_to_be_unique(
    df_temp_dq, TARGET_TABLE_NAME, False, BUSINESS_KEY, False
)
print(f"DQ result: {result}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Save to SA (Parquet, dynamic partition overwrite)

# COMMAND ----------

if result == "Success":
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    saveTable(TARGET_TABLE_NAME, "parquet", "overwrite", df_temp)
    print(f"SA write complete — total rows: {df_temp.count()}")
else:
    raise Exception(f"DQ check failed for {TARGET_TABLE_NAME} — SA write skipped")
