# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # PGC Market Proxy Price — Historical Fact Refinement
# MAGIC
# MAGIC The upstream framework drops `PGC Market Proxy Price File/Harmoni Tab.xlsx` into
# MAGIC storage with a **constant file name and sheet name** and converts it to parquet. The
# MAGIC source is a plain flat table with real column headers (`Customer`, `Plant Code`,
# MAGIC `Product`, `Price`, `Date`, `Shipment Type Filter`) — no in-data header row or
# MAGIC divider rows to locate.
# MAGIC
# MAGIC We don't know the exact day a new file appears, so this notebook **pings the source
# MAGIC daily**: the source carries its own `Date`; if that's not newer than the
# MAGIC `published_date` already stored, it exits immediately (fast & cheap) before doing any
# MAGIC further processing — otherwise it refines and appends.
# MAGIC
# MAGIC `material_id` is derived from the first 8 characters of `Product` (the material code),
# MAGIC zero-padded to 18 digits, and used to look up `material_desc` from `mat_attr_dim`.
# MAGIC
# MAGIC **Output:** `pgc_market_proxy_price_historical_fact` — a historical Delta table on
# MAGIC `mda-pipeline-refined`; each new published_date is appended as a snapshot.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# --- PARAMETERS (easy to change) ---

# Source parquet (constant name, content changes over time)
SOURCE_PARQUET_PATH = "/mnt/sharepoint/Foyer/PGC Market Proxy Price File/Harmoni Tab.parquet"

# Target historical fact table (Delta, append per new published_date)
TARGET_TABLE_NAME = "pgc_market_proxy_price_historical_fact"
TARGET_PATH = "/mnt/mda-pipeline-refined/pgc_market_proxy_price_historical_fact"
TEMP_PATH = "/mnt/mda-pipeline-temp/pgc_market_proxy_price_historical_fact"

# Direct column mapping: source_column -> target_column (Product handled separately below,
# it's not carried through as-is — material_id/material_desc are derived from it instead)
SOURCE_DATE_COL = "Date"  # our watermark, before renaming
COLUMN_MAPPING = {
    "Plant Code": "plant_code",
    "Customer": "customer",
    "Price": "price",
    SOURCE_DATE_COL: "published_date",
    "Shipment Type Filter": "shipment_type_filter",
}
PUBLISHED_DATE_COL = "published_date"

# material_id: first N chars of Product (the material code prefix), zero-padded to 18 digits
MATERIAL_CODE_LEN = 8
MATERIAL_ID_WIDTH = 18

# Material attribute dimension used to look up material_desc by material_id (left join —
# a material_id with no match in the dimension keeps its row, with material_desc = NULL)
MAT_ATTR_DIM_TABLE = "mat_attr_dim"
MAT_ATTR_DIM_KEY_COL = "material_id"
MAT_ATTR_DIM_DESC_COL = "material_desc"  # adjust here if the real column is material_description

# DQ uniqueness key — price is part of it: the same customer/plant/material/date/shipment
# type can legitimately carry more than one price, so this only catches truly identical rows.
BUSINESS_KEY = [
    "customer",
    "plant_code",
    "material_id",
    "shipment_type_filter",
    PUBLISHED_DATE_COL,
    "price",
]

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Read source and compute the new file's max published_date

# COMMAND ----------

pgc = spark.read.parquet(SOURCE_PARQUET_PATH)

new_max_published_date = pgc.agg(
    F.max(F.col(f"`{SOURCE_DATE_COL}`").cast("date")).alias("m")
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Select/rename columns, derive material_id, look up material_desc

# COMMAND ----------

# Select + rename in one step via backtick-quoted F.col (safe even if a name ever
# contains a dot or other special character — quotes the whole name as one literal).
df_data = pgc.select(
    *[F.col(f"`{src_col}`").alias(tgt_col) for src_col, tgt_col in COLUMN_MAPPING.items()],
    F.lpad(
        F.substring(F.trim(F.col("`Product`")), 1, MATERIAL_CODE_LEN), MATERIAL_ID_WIDTH, "0"
    ).alias("material_id"),
).withColumn(PUBLISHED_DATE_COL, F.col(PUBLISHED_DATE_COL).cast("date"))

mat_attr_dim = spark.table(MAT_ATTR_DIM_TABLE).select(
    F.col(MAT_ATTR_DIM_KEY_COL).alias("material_id"),
    F.col(MAT_ATTR_DIM_DESC_COL).alias("material_desc"),
)
pgc_market_proxy_price_historical_fact = df_data.join(mat_attr_dim, on="material_id", how="left")

print(f"Refined records: {pgc_market_proxy_price_historical_fact.count()}")
pgc_market_proxy_price_historical_fact.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Stage to temp, DQ check, append to historical Delta table

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
