# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # PGC Market Proxy Price — Historical Fact Refinement
# MAGIC
# MAGIC The upstream framework drops `PGC Market Proxy Price File.xlsx` into storage with a
# MAGIC **constant file name and sheet name** and converts it to parquet. Only the *content*
# MAGIC changes (a new monthly publication). We don't know the exact day a new file appears,
# MAGIC so this notebook **pings the source daily**: if there is no new publication it exits
# MAGIC immediately (fast & cheap); when a new publication appears it refines and appends.
# MAGIC
# MAGIC The raw parquet is "as-is": the real table headers live *inside* the data and the
# MAGIC columns are auto-named `Unnamed: 0..N`.
# MAGIC
# MAGIC Raw layout (located **by content**, not fixed row numbers — the file itself inserts
# MAGIC alignment rows, so positions shift):
# MAGIC - Publication banner, e.g. `... Published 19-December-2025`
# MAGIC - The real column header row (`PGC PC`, `PGC Brand Seg`, `Shipment Type`, ...)
# MAGIC - Data rows below the header
# MAGIC - **Divider rows**: author's section notes — only `Product` is populated, every other
# MAGIC   column is null. These are dropped.
# MAGIC
# MAGIC **Output:** `pgc_market_proxy_price_historical_fact` — a historical Delta table on
# MAGIC `mda-pipeline-refined`; each new publication is appended as a snapshot.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# --- PARAMETERS (easy to change) ---

# Source parquet (constant name, content changes monthly)
SOURCE_PARQUET_PATH = "/mnt/sharepoint/Foyer/PGC Market Proxy Price File/2526 Market Proxies.parquet"

# Target historical fact table (Delta, append per new publication)
TARGET_TABLE_NAME = "pgc_market_proxy_price_historical_fact"
TARGET_PATH = "/mnt/mda-pipeline-refined/pgc_market_proxy_price_historical_fact"
TEMP_PATH = "/mnt/mda-pipeline-temp/pgc_market_proxy_price_historical_fact"

# Text markers used to locate rows by content (case-insensitive)
PUBLISHED_MARKER = "Published"   # publication banner row
HEADER_MARKER = "PGC PC"         # first cell of the real header row

# Descriptive column that divider rows populate on their own
PRODUCT_COL = "Product"

# DQ uniqueness key
BUSINESS_KEY = ["Key", "processed_timestamp"]

# COMMAND ----------

from datetime import date, datetime
import re
from pyspark.sql import Row
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Read raw file and detect publication period

# COMMAND ----------

mp = spark.read.parquet(SOURCE_PARQUET_PATH)

# Find the publication banner cell (scan columns, stop at first match — cheap).
banner_text = None
for c in mp.columns:
    r = mp.filter(F.col(f"`{c}`").rlike(f"(?i).*{PUBLISHED_MARKER}.*")).select(F.col(f"`{c}`")).first()
    if r is not None:
        banner_text = r[0]
        break

# Parse 'Month-Year' — handles '...Published 19-December-2025' and '...Published December-2025'.
m = re.search(r"Published\s+(?:\d{1,2}-)?([A-Za-z]+)-(\d{4})", banner_text or "", re.IGNORECASE)
if not m:
    raise ValueError(f"Could not parse publication date from banner: {banner_text!r}")

published_year = int(m.group(2))
published_month = datetime.strptime(m.group(1)[:3], "%b").month  # 'Dec'/'December' -> 12
published_date = date(published_year, published_month, 1)
published_period = f"{published_date.strftime('%B')}-{published_year}"  # 'December-2025'
print(f"File publication period: {published_period} ({published_date})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Watermark check — new file or exit (daily ping, fast & cheap)

# COMMAND ----------

# Latest publication already stored in the target = watermark.
last_published = None
try:
    target_exists = any(
        f.name.endswith(".parquet") or "_delta_log" in f.name for f in dbutils.fs.ls(TARGET_PATH)
    )
except Exception:
    target_exists = False

if target_exists:
    df_target = spark.read.format("delta").load(TARGET_PATH)
    if "published_date" in df_target.columns:
        last_published = df_target.agg(F.max("published_date").alias("m")).first()["m"]

print(f"Last processed publication: {last_published}")

if last_published is not None and published_date <= last_published:
    print(f"No new content ({published_period} already processed). Exiting.")
    dbutils.notebook.exit(f"SKIP: {published_period} already processed")

print(f"New publication detected ({published_period}). Running refinement.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Add a stable row index and locate the header row

# COMMAND ----------

# Spark DataFrames are unordered; coalesce(1) + zipWithIndex preserves the original
# parquet order so we can find the header row and keep the rows below it.
original_cols = mp.columns  # the 'Unnamed: N' columns
mp_idx = spark.createDataFrame(
    mp.coalesce(1).rdd.zipWithIndex().map(lambda x: Row(**{**x[0].asDict(), "_row_idx": x[1]}))
)

header_row = (
    mp_idx.filter(F.col(f"`{original_cols[0]}`").rlike(f"(?i)^\\s*{HEADER_MARKER}\\s*$"))
    .orderBy("_row_idx")
    .first()
)
if header_row is None:
    raise ValueError(f"Header row not found (marker='{HEADER_MARKER}').")
header_idx = header_row["_row_idx"]

# Map 'Unnamed: N' -> real header, sanitized to a safe Delta/parquet column name.
header_map = {}
for c in original_cols:
    v = header_row[c]
    if v not in (None, ""):
        clean = re.sub(r"_+", "_", re.sub(r"[ ,;{}()\n\t=]+", "_", str(v).strip())).strip("_")
        header_map[c] = clean
clean_cols = list(header_map.values())
print(f"Header row index: {header_idx}; detected {len(clean_cols)} columns: {clean_cols}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Keep data rows (below header), rename to real headers, drop divider rows

# COMMAND ----------

df_data = mp_idx.filter(F.col("_row_idx") > header_idx)
for orig, new_name in header_map.items():
    df_data = df_data.withColumnRenamed(orig, new_name)
df_data = df_data.select(*clean_cols)

# Drop divider rows: author's section notes where ONLY `Product` is populated
# (every other column null). This also removes any fully-empty trailing rows.
non_product_cols = [c for c in clean_cols if c != PRODUCT_COL]
has_real_data = F.greatest(*[F.col(f"`{c}`").isNotNull().cast("int") for c in non_product_cols]) == 1
df_data = df_data.filter(has_real_data)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Add lineage columns → pgc_market_proxy_price_historical_fact

# COMMAND ----------

pgc_market_proxy_price_historical_fact = (
    df_data
    .withColumn("published_date", F.lit(published_date).cast("date"))
    .withColumn("processed_timestamp", F.current_timestamp())
)

print(f"Refined records: {pgc_market_proxy_price_historical_fact.count()}")
pgc_market_proxy_price_historical_fact.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Stage to temp, DQ check, append to historical Delta table

# COMMAND ----------

# Stage the current snapshot in temp, validate uniqueness, then append to the final table.
saveTabletemp(TARGET_TABLE_NAME, "delta", "overwrite", pgc_market_proxy_price_historical_fact)

df_temp = spark.read.format("delta").load(TEMP_PATH)
result = expect_table_records_to_be_unique(df_temp, TARGET_TABLE_NAME, False, BUSINESS_KEY, False)

if result == "Success":
    save_mode = "overwrite" if last_published is None else "append"
    saveTable(TARGET_TABLE_NAME, "delta", save_mode, df_temp)
    print(f"Saved {published_period} to {TARGET_TABLE_NAME} (mode={save_mode}).")
else:
    raise ValueError(f"DQ check failed for {published_period}: {result}")
