# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Delta Planning Parameters
# MAGIC
# MAGIC This notebook captures the initial MRP planning parameters (PDT, firm/trade-off zones, GR processing days, lot size, rounding value) for each plant-material combination at the moment it first appears in the source system, and preserves those baseline values for ongoing comparison with actual planning behavior.
# MAGIC
# MAGIC Loads planning parameters from `plant_material_dim` into `mm_init_planning_parameter_dim`.
# MAGIC - **First run**: full load (creates target table if not exists)
# MAGIC - **Ongoing runs**: INSERT only new combinations (LEFT ANTI JOIN on business key)
# MAGIC
# MAGIC **Strategy**: Only the first appearance of a business key is kept.
# MAGIC Parameters may change in source, but we intentionally preserve the initial values.
# MAGIC Runs daily on both dev and prod.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# --- PARAMETERS (easy to change) ---

# Source table (fully qualified)
SOURCE_TABLE = "app_fps_prod.inbound_supply_chain_shares.plant_material_dim"

# Target table (fully qualified)
TARGET_TABLE = "app_fps_prod.inbound_supply_chain_shares.mm_init_planning_parameter_dim"

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
# MAGIC ## Step 1: Read source data

# COMMAND ----------

from pyspark.sql import functions as F

df_source = spark.read.table(SOURCE_TABLE).filter(
    (F.col("source_system_code").isin(SOURCE_SYSTEM_CODES))
    & (F.length(F.col("material_id")) == 18)
)

# Apply column mapping (rename source columns to target names)
for src_col, tgt_col in COLUMN_MAPPING.items():
    df_source = df_source.withColumnRenamed(src_col, tgt_col)

# Select only mapped columns + add create_date
target_columns = list(COLUMN_MAPPING.values())
df_source = df_source.select(*target_columns).withColumn(
    "create_date", F.date_format(F.current_timestamp(), "yyyyMMdd")
)

# Deduplicate source on business key (keep first occurrence)
df_source = df_source.dropDuplicates(BUSINESS_KEY)

print(f"Source records (filtered & deduplicated): {df_source.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Check if target table exists

# COMMAND ----------

def table_exists(table_name: str) -> bool:
    """Check if a Delta table exists."""
    try:
        spark.read.table(table_name)
        return True
    except Exception:
        return False


target_exists = table_exists(TARGET_TABLE)
print(f"Target table '{TARGET_TABLE}' exists: {target_exists}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Initial load or Delta (INSERT new only)

# COMMAND ----------

if not target_exists:
    # --- INITIAL LOAD ---
    print("Performing INITIAL LOAD (target table does not exist)")

    df_source.write.format("delta").mode("overwrite").saveAsTable(TARGET_TABLE)

    print(f"Initial load complete. Records written: {df_source.count()}")

else:
    # --- DELTA LOAD (INSERT only new business keys) ---
    print("Performing DELTA LOAD (INSERT new combinations only)")

    df_target = spark.read.table(TARGET_TABLE)

    # LEFT ANTI JOIN: keep only source rows whose business key does NOT exist in target
    df_new = df_source.join(df_target, on=BUSINESS_KEY, how="left_anti")

    new_count = df_new.count()
    print(f"New records to insert: {new_count}")

    if new_count > 0:
        df_new.write.format("delta").mode("append").saveAsTable(TARGET_TABLE)
        print(f"Inserted {new_count} new records.")
    else:
        print("No new records to insert. Skipping.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Validation

# COMMAND ----------

df_result = spark.read.table(TARGET_TABLE)
total_count = df_result.count()

# Check for duplicates on business key
df_dupes = df_result.groupBy(*BUSINESS_KEY).count().filter(F.col("count") > 1)
dupe_count = df_dupes.count()

print(f"Total records in target: {total_count}")
print(f"Duplicate business keys:  {dupe_count}")

if dupe_count > 0:
    print("WARNING: Duplicates detected!")
    df_dupes.show(10, truncate=False)
else:
    print("OK: No duplicates found.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Preview

# COMMAND ----------

df_result.orderBy(*BUSINESS_KEY).show(20, truncate=False)
