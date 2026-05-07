# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Supplier FG Inventory vs IWL Daily Summary Fact (SQL version)
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
# MAGIC **Schedule**: every 2 hours
# MAGIC
# MAGIC **Strategy**
# MAGIC - First run → full load of the last 3 months
# MAGIC - Subsequent runs → recompute the last `BACKFILL_DAYS` snapshot dates and replaceWhere
# MAGIC - Sliding 3-month retention prune on temp Delta

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

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
LOOKBACK_MONTHS = 3
BACKFILL_DAYS   = 2

# ---------------------------------------------------------------------------
# Business key (used for DQ uniqueness check)
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

# ---------------------------------------------------------------------------
# Date boundaries (computed once per run, injected into SQL as literals)
# ---------------------------------------------------------------------------
today          = date.today()
cutoff_date    = today - timedelta(days=LOOKBACK_MONTHS * 30)
backfill_start = today - timedelta(days=BACKFILL_DAYS - 1)

# Make Python params visible to the %sql cells through SparkContext properties.
spark.conf.set("dl.fg_source_table",  FG_SOURCE_TABLE)
spark.conf.set("dl.iwl_source_table", IWL_SOURCE_TABLE)
spark.conf.set("dl.cutoff_date",      str(cutoff_date))
spark.conf.set("dl.backfill_start",   str(backfill_start))
spark.conf.set("dl.temp_path",        TEMP_PATH)

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
# MAGIC ## Step 1: Build the result via Spark SQL
# MAGIC
# MAGIC Single SQL statement that mirrors the original view, with two important changes:
# MAGIC 1. `BROADCAST(fg_suppliers)` hint — the distinct-key supplier table is small.
# MAGIC 2. `WHERE snapshot_date >= cutoff_date` is pushed into both source CTEs so the
# MAGIC    optimiser can prune source partitions.

# COMMAND ----------

result_sql = f"""
WITH fg_suppliers AS (
    SELECT DISTINCT
        purchase_vendor_id,
        business_unit_lkp_code,
        plant_code
    FROM {FG_SOURCE_TABLE}
),
fg AS (
    SELECT
        purchase_vendor_id,
        material_id,
        plant_code,
        CAST(snapshot_date AS DATE) AS snapshot_date,
        stock_type_desc,
        business_unit_lkp_code,
        SUM(quantity) AS quantity_ivy
    FROM {FG_SOURCE_TABLE}
    WHERE stock_type_desc IN ('Inventory On-Ground', 'Inventory In-Transit')
      AND snapshot_date >= DATE '{cutoff_date}'
    GROUP BY
        purchase_vendor_id,
        material_id,
        plant_code,
        CAST(snapshot_date AS DATE),
        stock_type_desc,
        business_unit_lkp_code
    HAVING SUM(quantity) > 0
),
iwl AS (
    SELECT
        purchase_vendor_id,
        material        AS material_id,
        plant           AS plant_code,
        calendar_date   AS snapshot_date,
        tdc_val         AS business_unit_lkp_code,
        SUM(buom_total_plant_stock) AS quantity_iwl
    FROM {IWL_SOURCE_TABLE}
    WHERE purchase_vendor_id IS NOT NULL
      AND calendar_date >= DATE '{cutoff_date}'
    GROUP BY
        purchase_vendor_id,
        material,
        plant,
        calendar_date,
        tdc_val
    HAVING SUM(buom_total_plant_stock) > 0
),
iwl_filtered AS (
    -- Keep only IWL rows whose (vendor, plant, BU) exists in FG.
    -- LEFT SEMI is the natural form of the original `fg_suppliers INNER JOIN iwl`,
    -- and BROADCAST avoids a shuffle since fg_suppliers is small.
    SELECT /*+ BROADCAST(fg_suppliers) */ iwl.*
    FROM iwl
    LEFT SEMI JOIN fg_suppliers
      ON fg_suppliers.purchase_vendor_id     = iwl.purchase_vendor_id
     AND fg_suppliers.plant_code             = iwl.plant_code
     AND fg_suppliers.business_unit_lkp_code = iwl.business_unit_lkp_code
)
SELECT
    COALESCE(fg.purchase_vendor_id,     iwl_filtered.purchase_vendor_id)     AS purchase_vendor_id,
    COALESCE(fg.material_id,            iwl_filtered.material_id)            AS material_id,
    COALESCE(fg.plant_code,             iwl_filtered.plant_code)             AS plant_code,
    COALESCE(fg.snapshot_date,          iwl_filtered.snapshot_date)          AS snapshot_date,
    COALESCE(fg.business_unit_lkp_code, iwl_filtered.business_unit_lkp_code) AS business_unit_lkp_code,
    fg.stock_type_desc,
    fg.quantity_ivy,
    iwl_filtered.quantity_iwl,
    current_timestamp() AS etl_load_ts
FROM fg
FULL OUTER JOIN iwl_filtered
  ON fg.purchase_vendor_id     = iwl_filtered.purchase_vendor_id
 AND fg.material_id            = iwl_filtered.material_id
 AND fg.plant_code             = iwl_filtered.plant_code
 AND fg.snapshot_date          = iwl_filtered.snapshot_date
 AND fg.business_unit_lkp_code = iwl_filtered.business_unit_lkp_code
"""

df_result = spark.sql(result_sql)
df_result.createOrReplaceTempView("v_result")

print(f"Result rows (full window): {spark.sql('SELECT COUNT(*) FROM v_result').collect()[0][0]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Determine load type and build delta

# COMMAND ----------

is_initial_load = not path_has_data(TEMP_PATH)

if is_initial_load:
    print("INITIAL LOAD — writing all 3-month data to temp")
    df_to_save = spark.sql("SELECT * FROM v_result")
else:
    print(f"DELTA LOAD — recomputing snapshot dates >= {backfill_start}")
    df_to_save = spark.sql(f"""
        SELECT *
        FROM v_result
        WHERE snapshot_date >= DATE '{backfill_start}'
    """)

df_to_save.createOrReplaceTempView("v_to_save")
new_count = spark.sql("SELECT COUNT(*) FROM v_to_save").collect()[0][0]
print(f"Rows to write: {new_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Save to temp (Delta, partition overwrite)

# COMMAND ----------

if new_count > 0:
    if is_initial_load:
        # First run — write the full 3-month window to temp.
        saveTabletemp(TARGET_TABLE_NAME, "delta", "overwrite", df_to_save)
    else:
        # Replace only the backfill date range in temp.
        # replaceWhere uses a data predicate — no partitionBy needed and it must
        # not be set here because the table schema (partitioning) was fixed at
        # initial-load time by saveTabletemp; adding partitionBy would conflict.
        (
            df_to_save.write
            .format("delta")
            .option("replaceWhere", f"{PARTITION_COL} >= '{backfill_start}'")
            .mode("overwrite")
            .save(TEMP_PATH)
        )
        print(f"Temp Delta written (replaceWhere >= {backfill_start})")
else:
    print("No new records — skipping temp write")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Sliding 3-month retention

# COMMAND ----------

if path_has_data(TEMP_PATH):
    spark.sql(f"""
        DELETE FROM delta.`{TEMP_PATH}`
        WHERE {PARTITION_COL} < DATE '{cutoff_date}'
    """)
    print(f"Retention prune complete (cutoff: {cutoff_date})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: DQ check

# COMMAND ----------

# stock_type_desc can be NULL (IWL-only rows); replace with sentinel so the
# uniqueness check treats NULLs as equal rather than always-distinct.
df_temp_dq = spark.sql(f"""
    SELECT
        snapshot_date,
        purchase_vendor_id,
        material_id,
        plant_code,
        business_unit_lkp_code,
        COALESCE(stock_type_desc, '__IWL_ONLY__') AS stock_type_desc,
        quantity_ivy,
        quantity_iwl,
        etl_load_ts
    FROM delta.`{TEMP_PATH}`
""")

result = expect_table_records_to_be_unique(
    df_temp_dq, TARGET_TABLE_NAME, False, BUSINESS_KEY, False
)
print(f"DQ result: {result}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Save to SA (Parquet, dynamic partition overwrite)

# COMMAND ----------

if result == "Success":
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    df_temp = spark.sql(f"SELECT * FROM delta.`{TEMP_PATH}`")
    saveTable(TARGET_TABLE_NAME, "parquet", "overwrite", df_temp)
    print(f"SA write complete — total rows: {df_temp.count()}")
else:
    raise Exception(f"DQ check failed for {TARGET_TABLE_NAME} — SA write skipped")
