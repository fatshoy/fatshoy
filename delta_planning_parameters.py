# Databricks notebook source
# MAGIC %md
# MAGIC ### Notebook: 2031_materialize_mm_init_planning_parameter_dim
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Business Context
# MAGIC Captures initial MRP planning parameters (PDT, firm/trade-off zones, GR processing days, lot size, rounding value) for each plant-material combination at the moment it first appears in `plant_material_dim` **and is active in `planning_parameter_fact`**, preserving baseline values for comparison.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Source Tables
# MAGIC | Source | Table | Mount / Path |
# MAGIC |--------|-------|-------------|
# MAGIC | SPPO | plant_material_dim | app_fps_prod.inbound_supply_chain_shares.plant_material_dim |
# MAGIC | RPM | planning_parameter_fact | /mnt/mda-pipeline-refined/planning_parameter_fact |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Additional Notes
# MAGIC
# MAGIC **Filters:**
# MAGIC - source_system_code IN box_filter
# MAGIC - length(material_id) == 18
# MAGIC - source_system_code + plant_code + material_id must exist in planning_parameter_fact (active combinations only)
# MAGIC
# MAGIC **Logic & Calculations:**
# MAGIC - Column mapping: planned_delivery_time_in_days -> pdt_days, locked_zone_code -> firm_zone_code, etc.
# MAGIC - Delta load: LEFT_ANTI JOIN for new records only (append); initial load uses overwrite
# MAGIC - Primary Key: source_system_code, site_code, material_id

# COMMAND ----------

# DBTITLE 1,Run DQ Config
# MAGIC %run "../../utilities/DQConfig"

# COMMAND ----------

# DBTITLE 1,Read Data
from pyspark.sql import functions as F

PRIMARY_KEYS = ["source_system_code", "site_code", "material_id"]

df_ppf = (
    spark.read.parquet("/mnt/mda-pipeline-refined/planning_parameter_fact")
    .select("source_system_code", "plant_code", "material_id")
    .distinct()
)

plant_material_dim = (
    spark.read.table("app_fps_prod.inbound_supply_chain_shares.plant_material_dim")
    .filter(
        (F.col("source_system_code").isin(box_filter))    # box_filters are from DQ config
        & (F.length(F.col("material_id")) == 18)
    )
)

df_source = plant_material_dim.join(
    df_ppf,
    on=["source_system_code", "plant_code", "material_id"],
    how="inner"
)

# COMMAND ----------

# DBTITLE 1,Data Transformation
# Source_column -> target_column
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

for src_col, tgt_col in COLUMN_MAPPING.items():
    df_source = df_source.withColumnRenamed(src_col, tgt_col)

target_columns = list(COLUMN_MAPPING.values())

df_source = (
    df_source.select(*target_columns)
    .dropDuplicates(PRIMARY_KEYS)
    .withColumn("create_date", F.date_format(F.current_timestamp(), "yyyyMMdd"))
)

# COMMAND ----------

# DBTITLE 1,Defining processing type
def path_has_data(path: str) -> bool:
    try:
        files = dbutils.fs.ls(path)
        return any(f.name.endswith(".parquet") or "_delta_log" in f.name for f in files)
    except Exception:
        return False

is_initial_load = not path_has_data("/mnt/mda-pipeline-temp/mm_init_planning_parameter_dim")

if is_initial_load:
    print("INITIAL LOAD (temp table does not exist)")
    df_to_save = df_source
    temp_save_mode = "overwrite"
else:
    print("DELTA LOAD (append new combinations only)")
    df_temp_existing = spark.read.format("delta").load("/mnt/mda-pipeline-temp/mm_init_planning_parameter_dim")
    df_to_save = df_source.join(df_temp_existing, on=PRIMARY_KEYS, how="left_anti")
    temp_save_mode = "append"

new_count = df_to_save.count()
print(f"New records: {new_count}")

# COMMAND ----------

# DBTITLE 1,Backup saving
if new_count > 0:
    saveTabletemp('mm_init_planning_parameter_dim', 'delta', temp_save_mode, df_to_save)
else:
    print("No new records. Skipping.")

# COMMAND ----------

# DBTITLE 1,DQ check and save to SA
if new_count > 0:
    df_temp = spark.read.format("delta").load("/mnt/mda-pipeline-temp/mm_init_planning_parameter_dim")
    result = expect_table_records_to_be_unique(df_temp, 'mm_init_planning_parameter_dim', False, PRIMARY_KEYS, True)
    if result == 'Success':
        saveTable('mm_init_planning_parameter_dim', 'parquet', 'overwrite', df_temp)
else:
    print("No new records. Skipping.")

# COMMAND ----------

# DBTITLE 1,Data Lineage
# mm_init_planning_parameter_dim = [["/mnt/mda-pipeline-refined/planning_parameter_fact",
#                                    "app_fps_prod.inbound_supply_chain_shares.plant_material_dim"],
#                                   ["/mnt/mda-pipeline-refined/mm_init_planning_parameter_dim/"]]
# source_target_logging(mm_init_planning_parameter_dim)

# COMMAND ----------

# DBTITLE 1,Notebook Exit
dbutils.notebook.exit(notebookReturnSuccess)
