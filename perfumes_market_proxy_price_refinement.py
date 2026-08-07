# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Perfumes Prices — Historical Fact Refinement
# MAGIC
# MAGIC The upstream framework drops two files into storage with **constant file names and
# MAGIC sheet names** and converts them to parquet: `INTERNAL PERFUMES.xlsx` and
# MAGIC `AROMA CHEMICAL W.A.xlsx`. Both already have real column headers — no in-data header
# MAGIC row or divider rows to locate. Each source is selected/renamed to the same common
# MAGIC schema and unioned together.
# MAGIC
# MAGIC We don't know the exact day new files appear, so this notebook **pings the source
# MAGIC daily**: each source carries its own `Process Date` (when it was sent to us); if the
# MAGIC newest one across both isn't newer than what's already stored, it exits immediately
# MAGIC (fast & cheap); when it is newer, it refines and appends.
# MAGIC
# MAGIC **Output:** `perfumes_market_proxy_price_historical_fact` — a historical Delta table
# MAGIC on `mda-pipeline-refined`; each new process_date is appended as a snapshot.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# --- PARAMETERS (easy to change) ---

# Source parquet paths (constant names, content changes over time)
INTERNAL_PERFUMES_PATH = "/mnt/sharepoint/Foyer/PRM_PERFUME_PRICES/INTERNAL PERFUMES.parquet"
AROMA_CHEMICALS_PATH = "/mnt/sharepoint/Foyer/PRM_PERFUME_PRICES/AROMA CHEMICAL W.A.parquet"

# Target historical fact table (Delta, append per new process_date)
TARGET_TABLE_NAME = "perfumes_market_proxy_price_historical_fact"
TARGET_PATH = "/mnt/mda-pipeline-refined/perfumes_market_proxy_price_historical_fact"
TEMP_PATH = "/mnt/mda-pipeline-temp/perfumes_market_proxy_price_historical_fact"

# Column mapping per source: source_column -> common target_column
INTERNAL_PERFUMES_MAPPING = {
    "GCAS": "material_id",
    "Perfume Name": "material_description",
    "PRODUCTION PLANT": "plant_code",
    "Perfume $/KG": "price_per_kg_usd",
    "Process Date": "process_date",  # yyyyMMdd date the file was sent to us — our watermark
}
AROMA_CHEMICALS_MAPPING = {
    "Material ID": "material_id",
    "Material Description": "material_description",
    "Plant Code": "plant_code",
    "W.A. PRICE PER KG USD": "price_per_kg_usd",
    "Process Date": "process_date",
}
PROCESS_DATE_COL = "process_date"

# DQ uniqueness key
BUSINESS_KEY = ["material_id", "plant_code", PROCESS_DATE_COL]

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Read both sources, select/rename to a common schema, and union

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

perfumes_market_proxy_price_historical_fact = df_internal_perfumes.unionByName(df_aroma_chemicals)

new_max_process_date = perfumes_market_proxy_price_historical_fact.agg(
    F.max(PROCESS_DATE_COL).alias("m")
).first()["m"]
print(f"New files' max {PROCESS_DATE_COL}: {new_max_process_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Watermark check — new data or exit (daily ping, fast & cheap)

# COMMAND ----------

# Latest process_date already stored in the target = watermark.
last_process_date = None
try:
    target_exists = any(
        f.name.endswith(".parquet") or "_delta_log" in f.name for f in dbutils.fs.ls(TARGET_PATH)
    )
except Exception:
    target_exists = False

if target_exists:
    df_target = spark.read.format("delta").load(TARGET_PATH)
    if PROCESS_DATE_COL in df_target.columns:
        last_process_date = df_target.agg(F.max(PROCESS_DATE_COL).alias("m")).first()["m"]

print(f"Last processed {PROCESS_DATE_COL}: {last_process_date}")

if last_process_date is not None and (
    new_max_process_date is None or new_max_process_date <= last_process_date
):
    print(f"No new content ({new_max_process_date} already processed). Exiting.")
    dbutils.notebook.exit(f"SKIP: {new_max_process_date} already processed")

print(f"New data detected ({new_max_process_date}). Running refinement.")
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
    save_mode = "overwrite" if not target_exists else "append"
    saveTable(TARGET_TABLE_NAME, "delta", save_mode, df_temp)
    print(f"Saved {new_max_process_date} to {TARGET_TABLE_NAME} (mode={save_mode}).")
else:
    raise ValueError(f"DQ check failed for {new_max_process_date}: {result}")
