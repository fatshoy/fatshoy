# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Delta Planning Parameters
# MAGIC
# MAGIC This notebook captures the initial MRP planning parameters (PDT, firm/trade-off zones, GR processing days, lot size, rounding value) for each plant-material combination at the moment it first appears **and is active in the planning parameter fact table**, preserving baseline values for ongoing comparison with actual planning behavior.
# MAGIC
# MAGIC Loads planning parameters from `plant_material_dim` into `mm_init_planning_parameter_dim`.
# MAGIC - **First run**: full load (creates target table if not exists)
# MAGIC - **Ongoing runs**: INSERT only new combinations (LEFT ANTI JOIN on business key)
# MAGIC
# MAGIC **Strategy**: Only the first appearance of a business key is kept, and only when the
# MAGIC `source_system_code + plant_code + material_id` combination exists in `planning_parameter_fact`
# MAGIC (i.e. the plant-material combination is active from a planning perspective).
# MAGIC Runs daily on both dev and prod.
# MAGIC
# MAGIC **Deployment note**: On first run after this logic change the temp table is wiped so that
# MAGIC all existing rows (captured under the old logic) are discarded and re-captured under the
# MAGIC new filter.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# --- PARAMETERS (easy to change) ---

# Source table (fully qualified)
SOURCE_TABLE = "app_fps_prod.inbound_supply_chain_shares.plant_material_dim"

# Planning parameter fact — used to filter only active plant-material combinations
PPF_PATH = "/mnt/mda-pipeline-refined/planning_parameter_fact"

# Target table name (used by saveTable/saveTabletemp)
TARGET_TABLE_NAME = "mm_init_planning_parameter_dim"
TARGET_PATH = "/mnt/mda-pipeline-refined/mm_init_planning_parameter_dim"
TEMP_PATH = "/mnt/mda-pipeline-temp/mm_init_planning_parameter_dim"

# Source system codes to process — extend this list as needed
SOURCE_SYSTEM_CODES = ["A6PS4H", "F6PS4H", "N6P420", "L6P430"]

# Column mapping: source_column -> target_column
COLUMN_MAPPING = {
    "source_system_code": "source_system_code",
    "plant_code": "site_code",
    "material_id": "material_id",
    "planned_delivery_time_in_days": "pdt_days",
    "locked_zone_code": "firm_zone_code",
    "high_cost_zone_code": "trade_off_zone_code",
    "goods_receipt_processing_time_in_days": "gr_process_days",
    "minimum_lot_size": "min_lot_size",
    "rounding_value_for_purchase_order_quantity": "round_value",
}

# Business key columns (used for deduplication / anti-join)
BUSINESS_KEY = ["source_system_code", "site_code", "material_id"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0: Clear existing data to restart with updated logic
# MAGIC
# MAGIC The filter criteria changed (active combinations only via `planning_parameter_fact`),
# MAGIC so previously captured rows are invalid. Wiping the temp table forces an initial load.

# COMMAND ----------

try:
    dbutils.fs.rm(TEMP_PATH, recurse=True)
    print(f"Temp table cleared: {TEMP_PATH}")
except Exception:
    print("Temp table did not exist — nothing to clear.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Read source data

# COMMAND ----------

from pyspark.sql import functions as F

# Active plant-material combinations from the planning parameter fact table
df_ppf = (
    spark.read.parquet(PPF_PATH)
    .select("source_system_code", "plant_code", "material_id")
    .distinct()
)

# Read plant_material_dim, apply box filter and length filter, then restrict to
# combinations that are active in planning_parameter_fact (INNER JOIN).
# The join uses pre-rename column names (plant_code) so it runs before COLUMN_MAPPING.
df_source = (
    spark.read.table(SOURCE_TABLE)
    .filter(
        (F.col("source_system_code").isin(SOURCE_SYSTEM_CODES))
        & (F.length(F.col("material_id")) == 18)
    )
    .join(df_ppf, on=["source_system_code", "plant_code", "material_id"], how="inner")
)

# Apply column mapping (rename source columns to target names)
for src_col, tgt_col in COLUMN_MAPPING.items():
    df_source = df_source.withColumnRenamed(src_col, tgt_col)

# Select only mapped columns, deduplicate on business key, then add create_date
# Order matters: dedup BEFORE adding create_date so the timestamp column does not
# artificially inflate rows for the same business key combination.
target_columns = list(COLUMN_MAPPING.values())
df_source = (
    df_source.select(*target_columns)
    .dropDuplicates(BUSINESS_KEY)
    .withColumn("create_date", F.date_format(F.current_timestamp(), "yyyyMMdd"))
)

print(f"Source records (filtered & deduplicated): {df_source.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Determine load type and build delta

# COMMAND ----------

def path_has_data(path: str) -> bool:
    try:
        files = dbutils.fs.ls(path)
        return any(f.name.endswith(".parquet") or "_delta_log" in f.name for f in files)
    except Exception:
        return False


is_initial_load = not path_has_data(TEMP_PATH)

if is_initial_load:
    print("INITIAL LOAD (temp table does not exist)")
    df_to_save = df_source
    temp_save_mode = "overwrite"
else:
    print("DELTA LOAD (append new combinations only)")
    df_temp_existing = spark.read.format("delta").load(TEMP_PATH)
    df_to_save = df_source.join(df_temp_existing, on=BUSINESS_KEY, how="left_anti")
    temp_save_mode = "append"

new_count = df_to_save.count()
print(f"New records: {new_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Save to temp (delta, append)

# COMMAND ----------

# Accumulate in temp as delta format (supports append without file fragmentation issues)
if new_count > 0:
    saveTabletemp(TARGET_TABLE_NAME, 'delta', temp_save_mode, df_to_save)
else:
    print("No new records. Skipping.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: DQ check and save to SA (parquet, overwrite)

# COMMAND ----------

# Read full accumulated dataset from temp, validate, and overwrite final parquet
if new_count > 0:
    df_temp = spark.read.format("delta").load(TEMP_PATH)
    result = expect_table_records_to_be_unique(df_temp, TARGET_TABLE_NAME, False, BUSINESS_KEY, False)
    if result == 'Success':
        saveTable(TARGET_TABLE_NAME, 'parquet', 'overwrite', df_temp)
else:
    print("No new records. Skipping.")
