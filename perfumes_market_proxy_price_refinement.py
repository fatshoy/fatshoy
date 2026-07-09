# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Perfumes Prices — Historical Fact Refinement
# MAGIC
# MAGIC The upstream framework drops two files into storage with **constant file names and
# MAGIC sheet names** and converts them to parquet: `INTERNAL PERFUMES.xlsx` and
# MAGIC `AROMA CHEMICAL W.A.xlsx`. Both already have real column headers — no in-data header
# MAGIC row or divider rows to locate. Each source is selected/renamed to the same common
# MAGIC schema, unioned together, and stamped with one lineage column.
# MAGIC
# MAGIC **Output:** `perfumes_market_proxy_price_historical_fact` — a historical Delta table
# MAGIC on `mda-pipeline-refined`; each run is appended as a snapshot.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# --- PARAMETERS (easy to change) ---

# Source parquet paths (constant names, content changes over time)
INTERNAL_PERFUMES_PATH = "/mnt/sharepoint/Foyer/PRM_PERFUME_PRICES/INTERNAL PERFUMES.parquet"
AROMA_CHEMICALS_PATH = "/mnt/sharepoint/Foyer/PRM_PERFUME_PRICES/AROMA CHEMICAL W.A.parquet"

# Target historical fact table (Delta, append per run)
TARGET_TABLE_NAME = "perfumes_market_proxy_price_historical_fact"
TARGET_PATH = "/mnt/mda-pipeline-refined/perfumes_market_proxy_price_historical_fact"
TEMP_PATH = "/mnt/mda-pipeline-temp/perfumes_market_proxy_price_historical_fact"

# Column mapping per source: source_column -> common target_column
INTERNAL_PERFUMES_MAPPING = {
    "GCAS": "material_id",
    "Perfume Name": "material_description",
    "PRODUCTION PLANT": "plant_code",
    "Perfume $/KG": "price_per_kg_usd",
}
AROMA_CHEMICALS_MAPPING = {
    "Material ID": "material_id",
    "Material Description": "material_description",
    "Plant Code": "plant_code",
    "W.A. PRICE PER KG USD": "price_per_kg_usd",
}

# DQ uniqueness key
BUSINESS_KEY = ["material_id", "plant_code", "processed_timestamp"]

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Read both sources, select and rename to a common schema

# COMMAND ----------

internal_perfumes = spark.read.parquet(INTERNAL_PERFUMES_PATH)
aroma_chemicals = spark.read.parquet(AROMA_CHEMICALS_PATH)

# Select + rename in one step via backtick-quoted F.col: a plain string select() splits
# on '.' as a nested-field path, which breaks on source names like 'W.A. PRICE PER KG USD'.
# Backticks around the whole name quote it as one literal identifier, dots included.
df_internal_perfumes = internal_perfumes.select(
    *[F.col(f"`{src_col}`").alias(tgt_col) for src_col, tgt_col in INTERNAL_PERFUMES_MAPPING.items()]
)
df_aroma_chemicals = aroma_chemicals.select(
    *[F.col(f"`{src_col}`").alias(tgt_col) for src_col, tgt_col in AROMA_CHEMICALS_MAPPING.items()]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Union both sources and add lineage

# COMMAND ----------

perfumes_market_proxy_price_historical_fact = df_internal_perfumes.unionByName(
    df_aroma_chemicals
).withColumn("processed_timestamp", F.current_timestamp())

print(f"Refined records: {perfumes_market_proxy_price_historical_fact.count()}")
perfumes_market_proxy_price_historical_fact.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Stage to temp, DQ check, append to historical Delta table

# COMMAND ----------

# Stage the current snapshot in temp, validate uniqueness, then append to the final table.
saveTabletemp(TARGET_TABLE_NAME, "delta", "overwrite", perfumes_market_proxy_price_historical_fact)

df_temp = spark.read.format("delta").load(TEMP_PATH)
result = expect_table_records_to_be_unique(df_temp, TARGET_TABLE_NAME, False, BUSINESS_KEY, False)

if result == "Success":
    try:
        target_exists = any(
            f.name.endswith(".parquet") or "_delta_log" in f.name for f in dbutils.fs.ls(TARGET_PATH)
        )
    except Exception:
        target_exists = False

    save_mode = "overwrite" if not target_exists else "append"
    saveTable(TARGET_TABLE_NAME, "delta", save_mode, df_temp)
    print(f"Saved to {TARGET_TABLE_NAME} (mode={save_mode}).")
else:
    raise ValueError(f"DQ check failed: {result}")
