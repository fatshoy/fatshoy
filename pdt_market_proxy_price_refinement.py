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
# MAGIC We don't know the exact day a new file appears, so this notebook **pings the source
# MAGIC daily**: the source carries its own `Process Date` (when the file was sent to us); if
# MAGIC that's not newer than what's already stored, it exits immediately (fast & cheap);
# MAGIC when it is newer, it refines and appends.
# MAGIC
# MAGIC **Output:** `pdt_market_proxy_price_historical_fact` — a historical Delta table on
# MAGIC `mda-pipeline-refined`; each new process_date is appended as a snapshot.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# --- PARAMETERS (easy to change) ---

# Source parquet (constant name, content changes over time)
SOURCE_PARQUET_PATH = "/mnt/sharepoint/Foyer/PDT_PRICES/Sheet1.parquet"

# Target historical fact table (Delta, append per new process_date)
TARGET_TABLE_NAME = "pdt_market_proxy_price_historical_fact"
TARGET_PATH = "/mnt/mda-pipeline-refined/pdt_market_proxy_price_historical_fact"
TEMP_PATH = "/mnt/mda-pipeline-temp/pdt_market_proxy_price_historical_fact"

# Column mapping: source_column -> target_column
COLUMN_MAPPING = {
    "GCAS": "material_id",
    "PDT NAME": "material_description",
    "Production Plant": "plant_code",
    "PMC $/KG:string": "price_per_kg_usd",
    "Process Date": "process_date",  # yyyyMMdd date the file was sent to us — our watermark
}
PROCESS_DATE_COL = "process_date"

# material_id (GCAS) is zero-padded to this width to match the standard material_id format
MATERIAL_ID_WIDTH = 18

# DQ uniqueness key
BUSINESS_KEY = ["material_id", "plant_code", PROCESS_DATE_COL]

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Read source, select and rename columns

# COMMAND ----------

pdt = spark.read.parquet(SOURCE_PARQUET_PATH)

# Select + rename in one step via backtick-quoted F.col: a plain string select() splits
# on '.' as a nested-field path, which would break on a source name containing a dot.
# Backticks around the whole name quote it as one literal identifier, dots included.
pdt_market_proxy_price_historical_fact = pdt.select(
    *[F.col(f"`{src_col}`").alias(tgt_col) for src_col, tgt_col in COLUMN_MAPPING.items()]
)

# Zero-pad material_id (GCAS) to the standard width, e.g. 21306845 -> 000000000021306845.
pdt_market_proxy_price_historical_fact = pdt_market_proxy_price_historical_fact.withColumn(
    "material_id", F.lpad(F.col("material_id").cast("string"), MATERIAL_ID_WIDTH, "0")
)

new_max_process_date = pdt_market_proxy_price_historical_fact.agg(
    F.max(PROCESS_DATE_COL).alias("m")
).first()["m"]
print(f"New file's max {PROCESS_DATE_COL}: {new_max_process_date}")

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
pdt_market_proxy_price_historical_fact.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Stage to temp, DQ check, append to historical Delta table

# COMMAND ----------

# Stage the current snapshot in temp, validate uniqueness, then append to the final table.
saveTabletemp(TARGET_TABLE_NAME, "delta", "overwrite", pdt_market_proxy_price_historical_fact)

df_temp = spark.read.format("delta").load(TEMP_PATH)
result = expect_table_records_to_be_unique(df_temp, TARGET_TABLE_NAME, False, BUSINESS_KEY, False)

if result == "Success":
    save_mode = "overwrite" if not target_exists else "append"
    saveTable(TARGET_TABLE_NAME, "delta", save_mode, df_temp)
    print(f"Saved {new_max_process_date} to {TARGET_TABLE_NAME} (mode={save_mode}).")
else:
    raise ValueError(f"DQ check failed for {new_max_process_date}: {result}")
