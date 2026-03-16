# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Delta Planning Parameters
# MAGIC
# MAGIC Loads planning parameters from `plant_material_dim` into `mm_init_planning_parameter_dim`.
# MAGIC - **First run**: full load (creates target table if not exists)
# MAGIC - **Ongoing runs**: delta load via MERGE (upsert by source_system_code + plant_code + material_id)
# MAGIC
# MAGIC ### Assumptions (to be confirmed with product owner):
# MAGIC 1. Parameters can change over time → MERGE with SCD Type 1 (overwrite)
# MAGIC 2. `mda` alias in original SQL is a typo → should be `pp`
# MAGIC 3. Notebook runs daily
# MAGIC 4. Source system codes list is parameterized
# MAGIC 5. Target table is auto-created on first run

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

# Business key columns (used for MERGE match condition)
BUSINESS_KEY = ["source_system_code", "site_code", "material_id"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Read source data

# COMMAND ----------

from pyspark.sql import functions as F

# Read source with filter
source_codes_str = ", ".join([f"'{c}'" for c in SOURCE_SYSTEM_CODES])

df_source = spark.read.table(SOURCE_TABLE).filter(
    F.col("source_system_code").isin(SOURCE_SYSTEM_CODES)
)

# Apply column mapping (rename source columns to target names)
for src_col, tgt_col in COLUMN_MAPPING.items():
    df_source = df_source.withColumnRenamed(src_col, tgt_col)

# Select only mapped columns + add create_date
target_columns = list(COLUMN_MAPPING.values())
df_source = df_source.select(*target_columns).withColumn(
    "create_date", F.date_format(F.current_timestamp(), "yyyyMMdd")
)

print(f"Source records (filtered): {df_source.count()}")

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
# MAGIC ## Step 3: Initial load or Delta (MERGE)

# COMMAND ----------

if not target_exists:
    # --- INITIAL LOAD ---
    print("Performing INITIAL LOAD (target table does not exist)")

    df_source.write.format("delta").mode("overwrite").saveAsTable(TARGET_TABLE)

    print(f"Initial load complete. Records written: {df_source.count()}")

else:
    # --- DELTA LOAD (MERGE / UPSERT) ---
    print("Performing DELTA LOAD (MERGE)")

    from delta.tables import DeltaTable

    dt_target = DeltaTable.forName(spark, TARGET_TABLE)

    # Build merge condition on business key
    merge_condition = " AND ".join(
        [f"target.{col} = source.{col}" for col in BUSINESS_KEY]
    )

    # MERGE: update existing rows, insert new ones
    dt_target.alias("target").merge(
        df_source.alias("source"),
        merge_condition,
    ).whenMatchedUpdate(
        set={
            col: f"source.{col}"
            for col in target_columns + ["create_date"]
            if col not in BUSINESS_KEY
        }
    ).whenNotMatchedInsertAll().execute()

    print("Delta MERGE complete.")

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
