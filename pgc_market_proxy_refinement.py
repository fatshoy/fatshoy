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
# MAGIC - **Divider rows**: author's section notes / visual separators — `Key` is null
# MAGIC   (unlike real data rows, which always carry a Key). These are dropped.
# MAGIC
# MAGIC Of the ~285 raw columns, only a fixed whitelist (`KEEP_COLUMNS`) of business columns
# MAGIC is kept — the rest are internal working formulas not needed downstream.
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

# Whitelist of business columns to keep, keyed by the header cell text (whitespace
# collapsed, case-insensitive). The raw file carries ~285 columns of internal working
# formulas — only these are needed downstream. A couple of header labels repeat verbatim
# elsewhere in the sheet for unrelated working columns (e.g. a second 'Currency' near
# TP_Local, a second 'Shipment Type'/'BU' further right) — matching takes the FIRST
# occurrence in file order and ignores later repeats of the same text.
KEEP_COLUMNS = {
    "pgc pc": "PGC_PC",
    "pgc brand seg": "PGC_Brand_Seg",
    "shipment type": "Shipment_Type",
    "product": "Product",
    "customer": "Customer",
    "pc": "PC",
    "key": "Key",
    "pc-prod": "PC_PROD",
    "pc-prod extended": "PC_PROD_EXTENDED",
    "bu": "BU",
    "mega": "MEGA",
    "region": "REGION",
    "category": "Category",
    "fy 25-26": "FY_25_26",
    "bulk /mt": "Bulk_mT",
    "sustainability costs": "Sustainability_Costs",
    "logistics /mt": "Logistics_mT",
    "fx adj. /mt": "FX_Adj_mT",
    "proc/ec4w /mt": "Proc_EC4W_mT",
    "delivery /mt": "Delivery_mT",
    "market proxy/mt": "Market_Proxy_mT",
    "local currency/mt": "Market_Proxy_Price_Local_Currency_mT",
    "currency": "Invoice_Currency",
    "local currency /mt": "OND_Transfer_Price_Local_Currency_mT",
}

# Business key column — divider/note rows never have one; real data rows always do
KEY_COL = "Key"

# DQ uniqueness key
BUSINESS_KEY = [KEY_COL, "processed_timestamp"]

# COMMAND ----------

from datetime import date, datetime
import re
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

# Parse the real publication date — handles '...Published 19-December-2025',
# '...Published 8-September-2025' and (fallback) '...Published December-2025'.
m = re.search(r"Published\s+(?:(\d{1,2})-)?([A-Za-z]+)-(\d{4})", banner_text or "", re.IGNORECASE)
if not m:
    raise ValueError(f"Could not parse publication date from banner: {banner_text!r}")

published_day = int(m.group(1)) if m.group(1) else 1  # fallback to 1st if no day in banner
published_month = datetime.strptime(m.group(2)[:3], "%b").month  # 'Dec'/'December' -> 12
published_year = int(m.group(3))
published_date = date(published_year, published_month, published_day)  # real publication date
published_period = published_date.strftime("%d-%B-%Y")  # '19-December-2025'
print(f"File publication date: {published_date} ({published_period})")

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
# MAGIC ## Step 3: Add a stable row index, locate the header row, and select needed columns

# COMMAND ----------

# Spark DataFrames are unordered; on a single partition (coalesce(1)) the built-in
# monotonically_increasing_id() yields a sequential index in parquet order. Unlike an
# RDD zipWithIndex, this is allowed on shared / Unity Catalog clusters.
original_cols = mp.columns  # the 'Unnamed: N' columns
mp_idx = mp.coalesce(1).withColumn("_row_idx", F.monotonically_increasing_id())

header_row = (
    mp_idx.filter(F.col(f"`{original_cols[0]}`").rlike(f"(?i)^\\s*{HEADER_MARKER}\\s*$"))
    .orderBy("_row_idx")
    .first()
)
if header_row is None:
    raise ValueError(f"Header row not found (marker='{HEADER_MARKER}').")
header_idx = header_row["_row_idx"]

# Map 'Unnamed: N' -> the target business name, matching against KEEP_COLUMNS only.
# Everything else in the raw header row (~285 columns of internal working formulas)
# is dropped right here — cheapest point to drop them, before any row filtering runs.
remaining = dict(KEEP_COLUMNS)  # popped on match so a later repeat of the same text is ignored
header_map = {}
for c in original_cols:
    v = header_row[c]
    if v in (None, ""):
        continue
    normalized = re.sub(r"\s+", " ", str(v).strip()).lower()
    if normalized in remaining:
        header_map[c] = remaining.pop(normalized)

if remaining:
    raise ValueError(f"Expected column(s) not found in source header row: {sorted(remaining.values())}")

clean_cols = list(header_map.values())
print(f"Header row index: {header_idx}; keeping {len(clean_cols)} columns: {clean_cols}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Keep data rows (below header), rename to real headers, drop divider rows (Key is null)

# COMMAND ----------

df_data = mp_idx.filter(F.col("_row_idx") > header_idx)
for orig, new_name in header_map.items():
    df_data = df_data.withColumnRenamed(orig, new_name)
df_data = df_data.select(*clean_cols)

# Drop divider rows: author's section notes / blanks never carry a Key
# (Product itself can hold junk like '...', '0', 'Need Currency', or a note).
# This also removes any fully-empty trailing rows.
df_data = df_data.filter(F.col(f"`{KEY_COL}`").isNotNull())

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
