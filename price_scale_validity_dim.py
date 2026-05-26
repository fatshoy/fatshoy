# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Price Scale Validity Dim
# MAGIC
# MAGIC Materializes `price_scale_validity_dim` from the SQL Server view
# MAGIC `rpm.price_scale_validity_dim_vw`.
# MAGIC
# MAGIC For each purchase document line the query selects the price scale rows
# MAGIC that belong to the **latest** validity period (`MAX(valid_end_date)` per
# MAGIC document line), implemented as a single-pass window function to avoid
# MAGIC the double scan of the original view.
# MAGIC
# MAGIC - **Every run**: full reload — the whole table is recomputed and
# MAGIC   overwritten. No incremental logic applies here because the output is
# MAGIC   a current-state dimension driven by a global `MAX` aggregate; any
# MAGIC   weekly change can affect any row regardless of its validity date.
# MAGIC
# MAGIC Both temp staging and final SA are written as **parquet** (required for
# MAGIC downstream ASDW consumption).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

TARGET_TABLE_NAME = "price_scale_validity_dim"
TEMP_PATH         = "/mnt/mda-pipeline-temp/price_scale_validity_dim"

SOURCE_PATHS = {
    "price_validity_dim": "/mnt/mda-pipeline-refined/price_validity_dim",
    "price_scale_dim":    "/mnt/mda-pipeline-refined/price_scale_dim",
}

SOURCE_SYSTEM_CODES = ("'F6PS4H'", "'A6PS4H'", "'L6P430'", "'N6P420'")

# Business key: one price scale row per document line + quantity break.
BUSINESS_KEY = [
    "source_system_code",
    "purchase_doc_num",
    "purchase_doc_line_num",
    "scale_quantity",
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Register source temp views

# COMMAND ----------

for view_name, path in SOURCE_PATHS.items():
    spark.read.parquet(path).createOrReplaceTempView(view_name)
    print(f"Registered view {view_name} <- {path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Build result
# MAGIC
# MAGIC Single-pass rewrite of the original two-CTE pattern:
# MAGIC - `RANK()` over `valid_end_date DESC` replaces the `MAX` + self-join,
# MAGIC   eliminating a full second scan of `price_validity_dim`.
# MAGIC - `RANK` (not `ROW_NUMBER`) preserves ties — identical behaviour to the
# MAGIC   original `JOIN ON valid_end_date = max_valid_end_date` when multiple
# MAGIC   `cond_rec_num` share the same maximum date.
# MAGIC - Source system filter is pushed to both tables so each scan is pruned
# MAGIC   before the join.

# COMMAND ----------

ssc_filter = ", ".join(SOURCE_SYSTEM_CODES)

query = f"""
WITH pvd_ranked AS (
    SELECT
        source_system_code,
        purchase_doc_num,
        purchase_doc_line_num,
        cond_rec_num,
        valid_end_date,
        RANK() OVER (
            PARTITION BY source_system_code, purchase_doc_num, purchase_doc_line_num
            ORDER BY valid_end_date DESC
        ) AS rnk
    FROM price_validity_dim
    WHERE source_system_code IN ({ssc_filter})
)
SELECT
    psd.source_system_code,
    psd.purchase_doc_num,
    psd.purchase_doc_line_num,
    psd.valid_end_date,
    psd.scale_quantity,
    psd.Rate AS rate
FROM price_scale_dim psd
INNER JOIN pvd_ranked crn
    ON  crn.source_system_code    = psd.source_system_code
    AND crn.purchase_doc_num      = psd.purchase_doc_num
    AND crn.purchase_doc_line_num = psd.purchase_doc_line_num
    AND crn.cond_rec_num          = psd.cond_rec_num
    AND crn.valid_end_date        = psd.valid_end_date
WHERE crn.rnk = 1
  AND psd.deletion_ind <> 'X'
  AND psd.source_system_code IN ({ssc_filter})
"""

df_result = spark.sql(query)
row_count = df_result.count()
print(f"Result rows: {row_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Save to temp (parquet, overwrite)

# COMMAND ----------

if row_count > 0:
    saveTabletemp(TARGET_TABLE_NAME, "parquet", "overwrite", df_result)
    print("Temp parquet written")
else:
    raise ValueError("Query returned 0 rows — aborting to avoid overwriting SA with empty data")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: DQ check and save to SA (parquet, overwrite)

# COMMAND ----------

df_temp = spark.read.parquet(TEMP_PATH)

result = expect_table_records_to_be_unique(
    df_temp, TARGET_TABLE_NAME, False, BUSINESS_KEY, False
)
if result == "Success":
    saveTable(TARGET_TABLE_NAME, "parquet", "overwrite", df_temp)
