# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # PGC Market Proxy Price Refinement
# MAGIC
# MAGIC The upstream framework drops `PGC Market Proxy Price File.xlsx` into storage with a
# MAGIC **constant file name and sheet name** and converts it to parquet. Only the *content*
# MAGIC changes (a new monthly publication). The raw parquet is "as-is": the real table
# MAGIC headers live *inside* the data and the columns are auto-named `Unnamed: 0..N`.
# MAGIC
# MAGIC Raw layout (see source file):
# MAGIC - **Row 5**, col `Unnamed: 0` — publication banner, e.g. `... Published 19-December-2025`
# MAGIC - **Row 9** — the real column headers (`PGC PC`, `PGC Brand Seg`, `Shipment Type`, ...)
# MAGIC - **Row 10+** — the actual data
# MAGIC
# MAGIC **Strategy (runs daily):**
# MAGIC 1. Parse the publication `Month-Year` from the banner row.
# MAGIC 2. Compare against the latest publication already stored in the target table (watermark).
# MAGIC    - Not newer → **skip everything** (we already processed this month).
# MAGIC    - Newer (or target empty) → run the refinement.
# MAGIC 3. Refine: promote row 9 to column names, keep rows 10+, add lineage columns.
# MAGIC 4. Append the new monthly snapshot to the target Delta table.
# MAGIC
# MAGIC The banner row and header row are located **by content** (`Published`, `PGC PC`),
# MAGIC not by hard-coded row numbers, because the source file itself notes that extra rows
# MAGIC may be inserted ("extra row inserted so Reconciliation rows are same...").

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# --- PARAMETERS (easy to change) ---

# Source parquet (constant name, content changes monthly)
SOURCE_PARQUET_PATH = "/mnt/sharepoint/Foyer/PGC Market Proxy Price File/2526 Market Proxies.parquet"

# Target table (one monthly snapshot appended per new publication)
TARGET_TABLE_NAME = "pgc_market_proxy_price"
TARGET_PATH = "/mnt/mda-pipeline-refined/pgc_market_proxy_price"

# Text markers used to locate rows by content (case-insensitive)
PUBLISHED_MARKER = "Published"   # publication banner row
HEADER_MARKER = "PGC PC"         # first cell of the real header row

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helpers

# COMMAND ----------

import re
from datetime import date, datetime
from pyspark.sql import Row
from pyspark.sql import functions as F


def path_has_data(path: str) -> bool:
    """True if a delta/parquet dataset already exists at path."""
    try:
        files = dbutils.fs.ls(path)
        return any(f.name.endswith(".parquet") or "_delta_log" in f.name for f in files)
    except Exception:
        return False


def with_row_index(df, name: str = "_row_idx"):
    """Add a stable, file-order row index.

    Spark DataFrames are unordered, but we need 'banner row', 'header row' and
    'rows below the header'. coalesce(1) + zipWithIndex preserves the original
    parquet order for a single small file.
    """
    rdd = df.coalesce(1).rdd.zipWithIndex()
    rdd = rdd.map(lambda x: Row(**{**x[0].asDict(), name: x[1]}))
    return spark.createDataFrame(rdd)


def parse_published_date(banner_text: str):
    """Extract (period_label, period_date) from the publication banner.

    Handles '...Published 19-December-2025' and '...Published December-2025'.
    Returns ('December-2025', date(2025, 12, 1)) or (None, None) if not found.
    """
    if not banner_text:
        return None, None
    m = re.search(r"Published\s+(?:\d{1,2}-)?([A-Za-z]+)-(\d{4})", banner_text, re.IGNORECASE)
    if not m:
        return None, None
    month_name, year = m.group(1), int(m.group(2))
    month_num = datetime.strptime(month_name[:3], "%b").month  # 'Dec'/'December' -> 12
    label = f"{date(year, month_num, 1).strftime('%B')}-{year}"  # canonical 'December-2025'
    return label, date(year, month_num, 1)


def find_cell_containing(df, marker: str):
    """Return the first cell value (across all columns) matching `marker`, else None."""
    pattern = f"(?i).*{re.escape(marker)}.*"
    for c in df.columns:
        row = df.filter(F.col(f"`{c}`").rlike(pattern)).select(F.col(f"`{c}`")).first()
        if row is not None:
            return row[0]
    return None


def sanitize(name: str) -> str:
    """Make a header value safe as a Delta/parquet column name."""
    name = re.sub(r"[ ,;{}()\n\t=]+", "_", str(name).strip())
    return re.sub(r"_+", "_", name).strip("_")


def get_last_published_date(path: str):
    """Max publication date already stored in the target (the watermark), else None."""
    if not path_has_data(path):
        return None
    df = spark.read.format("delta").load(path)
    if "published_date" not in df.columns:
        return None
    return df.agg(F.max("published_date").alias("m")).first()["m"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Read raw file and detect publication period

# COMMAND ----------

mp = spark.read.parquet(SOURCE_PARQUET_PATH)

banner_text = find_cell_containing(mp, PUBLISHED_MARKER)
published_period, published_date = parse_published_date(banner_text)

if published_period is None:
    raise ValueError(
        f"Could not parse publication date from banner. "
        f"Marker='{PUBLISHED_MARKER}', found cell={banner_text!r}"
    )

print(f"File publication period: {published_period} ({published_date})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Watermark check — new file or skip?

# COMMAND ----------

last_published = get_last_published_date(TARGET_PATH)
print(f"Last processed publication: {last_published}")

is_new_publication = (last_published is None) or (published_date > last_published)

if not is_new_publication:
    print(f"No new content ({published_period} already processed). Skipping all refinement steps.")
    dbutils.notebook.exit(f"SKIP: {published_period} already processed")

print(f"New publication detected ({published_period}). Running refinement.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Refine — promote header row, keep data rows

# COMMAND ----------

mp_idx = with_row_index(mp)
original_cols = mp.columns  # the 'Unnamed: N' columns (excludes _row_idx)

# Locate the header row (row 9) by content.
header_row = (
    mp_idx.filter(F.col(f"`{original_cols[0]}`").rlike(f"(?i)^\\s*{re.escape(HEADER_MARKER)}\\s*$"))
    .orderBy("_row_idx")
    .first()
)
if header_row is None:
    raise ValueError(f"Header row not found (marker='{HEADER_MARKER}').")

header_idx = header_row["_row_idx"]
header_map = {c: header_row[c] for c in original_cols if header_row[c] not in (None, "")}
print(f"Header row index: {header_idx}; detected {len(header_map)} columns")

# Keep only rows below the header (row 10+), rename to real headers.
df_data = mp_idx.filter(F.col("_row_idx") > header_idx)
for orig, new_name in header_map.items():
    df_data = df_data.withColumnRenamed(orig, sanitize(new_name))

clean_cols = [sanitize(v) for v in header_map.values()]
df_data = df_data.select(*clean_cols)

# Drop fully-empty rows (trailing blanks from Excel).
df_data = df_data.dropna(how="all")

# Add lineage columns.
df_refined = (
    df_data
    .withColumn("published_period", F.lit(published_period))
    .withColumn("published_date", F.lit(published_date).cast("date"))
    .withColumn("create_date", F.date_format(F.current_timestamp(), "yyyyMMdd"))
)

print(f"Refined records: {df_refined.count()}")
df_refined.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Append the new monthly snapshot to the target Delta table

# COMMAND ----------

save_mode = "overwrite" if last_published is None else "append"
saveTable(TARGET_TABLE_NAME, "delta", save_mode, df_refined)
print(f"Saved {published_period} to {TARGET_TABLE_NAME} (mode={save_mode}).")
