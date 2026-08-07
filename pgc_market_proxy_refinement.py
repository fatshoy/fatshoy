# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # PGC Market Proxy Price — Historical Fact Refinement
# MAGIC
# MAGIC The upstream framework drops `PGC Market Proxy Price File.xlsx` into storage with a
# MAGIC **constant file name and sheet name** and converts it to parquet. The source now
# MAGIC ships with real column headers (`Plant ID`, `Product`, `Price`, `Published Date`) —
# MAGIC a plain flat table, no in-data header row or divider rows to locate.
# MAGIC
# MAGIC We don't know the exact day a new file appears, so this notebook **pings the source
# MAGIC daily**: the source carries its own `Published Date`; if that's not newer than what's
# MAGIC already stored, it exits immediately (fast & cheap); when it is newer, it refines and
# MAGIC appends. Rows are kept as-is — e.g. the same Plant/Product/Published Date can
# MAGIC legitimately appear with more than one Price, so `price` is part of the business key.
# MAGIC
# MAGIC **Output:** `pgc_market_proxy_price_historical_fact` — a historical Delta table on
# MAGIC `mda-pipeline-refined`; each new published_date is appended as a snapshot.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# --- PARAMETERS (easy to change) ---

# Source parquet (constant name, content changes over time)
SOURCE_PARQUET_PATH = "/mnt/sharepoint/Foyer/PGC Market Proxy Price File/2526 Market Proxies.parquet"

# Target historical fact table (Delta, append per new published_date)
TARGET_TABLE_NAME = "pgc_market_proxy_price_historical_fact"
TARGET_PATH = "/mnt/mda-pipeline-refined/pgc_market_proxy_price_historical_fact"
TEMP_PATH = "/mnt/mda-pipeline-temp/pgc_market_proxy_price_historical_fact"

# Column mapping: source_column -> target_column
COLUMN_MAPPING = {
    "Plant ID": "plant_code",
    "Product": "product",  # kept as a single field: source combines code + description
    "Price": "price",
    "Published Date": "published_date",  # our watermark
}
PUBLISHED_DATE_COL = "published_date"

# DQ uniqueness key — price is part of it: the same plant/product/date can legitimately
# carry more than one price, so this only catches truly identical duplicate rows.
BUSINESS_KEY = ["plant_code", "product", PUBLISHED_DATE_COL, "price"]

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Read source, select and rename columns

# COMMAND ----------

pgc = spark.read.parquet(SOURCE_PARQUET_PATH)

# Select + rename in one step via backtick-quoted F.col (safe even if a name ever
# contains a dot or other special character — quotes the whole name as one literal).
pgc_market_proxy_price_historical_fact = pgc.select(
    *[F.col(f"`{src_col}`").alias(tgt_col) for src_col, tgt_col in COLUMN_MAPPING.items()]
).withColumn(PUBLISHED_DATE_COL, F.col(f"`{PUBLISHED_DATE_COL}`").cast("date"))

new_max_published_date = pgc_market_proxy_price_historical_fact.agg(
    F.max(PUBLISHED_DATE_COL).alias("m")
).first()["m"]
print(f"New file's max {PUBLISHED_DATE_COL}: {new_max_published_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Watermark check — new data or exit (daily ping, fast & cheap)

# COMMAND ----------

# Latest published_date already stored in the target = watermark.
last_published_date = None
try:
    target_exists = any(
        f.name.endswith(".parquet") or "_delta_log" in f.name for f in dbutils.fs.ls(TARGET_PATH)
    )
except Exception:
    target_exists = False

if target_exists:
    df_target = spark.read.format("delta").load(TARGET_PATH)
    if PUBLISHED_DATE_COL in df_target.columns:
        last_published_date = df_target.agg(F.max(PUBLISHED_DATE_COL).alias("m")).first()["m"]

print(f"Last processed {PUBLISHED_DATE_COL}: {last_published_date}")

if last_published_date is not None and (
    new_max_published_date is None or new_max_published_date <= last_published_date
):
    print(f"No new content ({new_max_published_date} already processed). Exiting.")
    dbutils.notebook.exit(f"SKIP: {new_max_published_date} already processed")

print(f"New data detected ({new_max_published_date}). Running refinement.")
pgc_market_proxy_price_historical_fact.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Stage to temp, DQ check, append to historical Delta table

# COMMAND ----------

# Stage the current snapshot in temp, validate uniqueness, then append to the final table.
saveTabletemp(TARGET_TABLE_NAME, "delta", "overwrite", pgc_market_proxy_price_historical_fact)

df_temp = spark.read.format("delta").load(TEMP_PATH)
result = expect_table_records_to_be_unique(df_temp, TARGET_TABLE_NAME, False, BUSINESS_KEY, False)

if result == "Success":
    save_mode = "overwrite" if not target_exists else "append"
    saveTable(TARGET_TABLE_NAME, "delta", save_mode, df_temp)
    print(f"Saved {new_max_published_date} to {TARGET_TABLE_NAME} (mode={save_mode}).")
else:
    raise ValueError(f"DQ check failed for {new_max_published_date}: {result}")
