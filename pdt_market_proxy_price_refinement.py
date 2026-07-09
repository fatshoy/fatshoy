# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # PDT Prices — Historical Fact Refinement
# MAGIC
# MAGIC The upstream framework drops `PDT_PRICES/Sheet1.xlsx` into storage with a
# MAGIC **constant file name and sheet name** and converts it to parquet. Unlike the PGC
# MAGIC Market Proxy file, this source already has real column headers — no in-data header
# MAGIC row or divider rows to locate, so refinement is a straight select + rename.
# MAGIC
# MAGIC **Output:** `pdt_market_proxy_price_historical_fact` — a historical Delta table on
# MAGIC `mda-pipeline-refined`; each run is appended as a snapshot.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# --- PARAMETERS (easy to change) ---

# Source parquet (constant name, content changes over time)
SOURCE_PARQUET_PATH = "/mnt/sharepoint/Foyer/PDT_PRICES/Sheet1.parquet"

# Target historical fact table (Delta, append per run)
TARGET_TABLE_NAME = "pdt_market_proxy_price_historical_fact"
TARGET_PATH = "/mnt/mda-pipeline-refined/pdt_market_proxy_price_historical_fact"
TEMP_PATH = "/mnt/mda-pipeline-temp/pdt_market_proxy_price_historical_fact"

# Column mapping: source_column -> target_column
COLUMN_MAPPING = {
    "GCAS": "material_id",
    "PDT NAME": "material_description",
    "Production Plant": "plant_code",
    "PMC $/KG:string": "price_per_kg_usd",
}

# DQ uniqueness key
BUSINESS_KEY = ["material_id", "plant_code", "processed_timestamp"]

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Read source, select and rename columns, add lineage

# COMMAND ----------

pdt = spark.read.parquet(SOURCE_PARQUET_PATH)

df_data = pdt.select(*COLUMN_MAPPING.keys())
for src_col, tgt_col in COLUMN_MAPPING.items():
    df_data = df_data.withColumnRenamed(src_col, tgt_col)

pdt_market_proxy_price_historical_fact = df_data.withColumn(
    "processed_timestamp", F.current_timestamp()
)

print(f"Refined records: {pdt_market_proxy_price_historical_fact.count()}")
pdt_market_proxy_price_historical_fact.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Stage to temp, DQ check, append to historical Delta table

# COMMAND ----------

# Stage the current snapshot in temp, validate uniqueness, then append to the final table.
saveTabletemp(TARGET_TABLE_NAME, "delta", "overwrite", pdt_market_proxy_price_historical_fact)

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
