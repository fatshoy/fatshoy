# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Supplier FG Inventory vs IWL Daily Summary Fact (SQL version)
# MAGIC
# MAGIC Materialises the logic previously handled by `rpm.supplier_fg_inv_vs_iwl_daily_summary_vw`
# MAGIC into a partitioned Delta / Parquet fact table consumed by the RPMA AAS model.
# MAGIC
# MAGIC **Sources**
# MAGIC - `supplier_fg_inventory_fact` — finished-goods inventory snapshots (snapshot_date is INT yyyyMMdd)
# MAGIC - `iwl_daily_fact` — in-warehouse-location daily stock (calendar_date is DATE)
# MAGIC
# MAGIC **Target**: `rpm.supplier_fg_inv_vs_iwl_daily_summary_fact`
# MAGIC
# MAGIC **Schedule**: every 2 hours
# MAGIC
# MAGIC **Strategy**
# MAGIC - First run → full load of the last `LOOKBACK_MONTHS` months
# MAGIC - Subsequent runs → recompute only snapshot dates >= `backfill_start` and replaceWhere
# MAGIC - Sliding `LOOKBACK_MONTHS`-month retention prune on temp Delta

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

FG_SOURCE_TABLE  = "<catalog>.<schema>.supplier_fg_inventory_fact"   # PLACEHOLDER
IWL_SOURCE_TABLE = "<catalog>.<schema>.iwl_daily_fact"               # PLACEHOLDER

TARGET_TABLE_NAME = "supplier_fg_inv_vs_iwl_daily_summary_fact"
TARGET_PATH       = "/mnt/mda-pipeline-refined/supplier_fg_inv_vs_iwl_daily_summary_fact"
TEMP_PATH         = "/mnt/mda-pipeline-temp/supplier_fg_inv_vs_iwl_daily_summary_fact"

LOOKBACK_MONTHS = 3
BACKFILL_DAYS   = 2

BUSINESS_KEY = [
    "snapshot_date",
    "purchase_vendor_id",
    "material_id",
    "plant_code",
    "business_unit_lkp_code",
    "stock_type_desc",
]

PARTITION_COL = "snapshot_date"

cutoff_date    = spark.sql(f"SELECT add_months(current_date(), -{LOOKBACK_MONTHS})").first()[0]
backfill_start = spark.sql(f"SELECT date_sub(current_date(), {BACKFILL_DAYS - 1})").first()[0]
cutoff_int     = int(cutoff_date.strftime("%Y%m%d"))

print(f"cutoff_date    = {cutoff_date}")
print(f"backfill_start = {backfill_start}")

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
# MAGIC Optimisations vs. legacy view:
# MAGIC 1. `BROADCAST(fg_suppliers)` hint — the distinct-key supplier table is small.
# MAGIC 2. `LEFT SEMI JOIN fg_suppliers` instead of `INNER JOIN` — same filter semantics, no row duplication.
# MAGIC 3. FG `WHERE` uses raw `snapshot_date > <INT>` so Spark can prune source partitions
# MAGIC    (partition pruning requires comparison on the raw column, not on `to_date(...)`).
# MAGIC 4. Strict `>` on cutoff to exclude the boundary day, matching ASDW T-SQL semantics.

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
        to_date(CAST(snapshot_date AS STRING), 'yyyyMMdd') AS snapshot_date,
        stock_type_desc,
        business_unit_lkp_code,
        SUM(quantity) AS quantity_ivy
    FROM {FG_SOURCE_TABLE}
    WHERE stock_type_desc IN ('Inventory On-Ground', 'Inventory In-Transit')
      AND snapshot_date > {cutoff_int}
    GROUP BY
        purchase_vendor_id,
        material_id,
        plant_code,
        to_date(CAST(snapshot_date AS STRING), 'yyyyMMdd'),
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
      AND calendar_date > DATE '{cutoff_date}'
    GROUP BY
        purchase_vendor_id,
        material,
        plant,
        calendar_date,
        tdc_val
    HAVING SUM(buom_total_plant_stock) > 0
),
iwl_filtered AS (
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
# MAGIC
# MAGIC - **Initial load**: temp path empty → write the whole window.
# MAGIC - **Delta load**: temp path has data → recompute only `snapshot_date >= backfill_start`
# MAGIC   so we touch only the partitions that actually changed.

# COMMAND ----------

is_initial_load = not path_has_data(TEMP_PATH)

if is_initial_load:
    print(f"INITIAL LOAD — writing full window ({cutoff_date} < snapshot_date) to temp")
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
# MAGIC ## Step 3: Save to temp (Delta, replaceWhere on backfill range)

# COMMAND ----------

if new_count > 0:
    if is_initial_load:
        saveTabletemp(TARGET_TABLE_NAME, "delta", "overwrite", df_to_save)
    else:
        (
            df_to_save.write
            .format("delta")
            .option("replaceWhere", f"{PARTITION_COL} >= '{backfill_start}'")
            .mode("overwrite")
            .save(TEMP_PATH)
        )
        print(f"Temp Delta written (replaceWhere {PARTITION_COL} >= '{backfill_start}')")
else:
    print("No new records — skipping temp write")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Sliding retention — drop partitions outside the window
# MAGIC
# MAGIC The data SQL keeps `snapshot_date > cutoff_date`, so anything `<= cutoff_date`
# MAGIC in temp is stale and can be removed.

# COMMAND ----------

if path_has_data(TEMP_PATH):
    spark.sql(f"""
        DELETE FROM delta.`{TEMP_PATH}`
        WHERE {PARTITION_COL} <= DATE '{cutoff_date}'
    """)
    print(f"Retention prune complete (kept: snapshot_date > {cutoff_date})")

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
