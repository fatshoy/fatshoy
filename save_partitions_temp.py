# Databricks notebook source

# MAGIC %md
# MAGIC # Config helper: saveTabletempPartitions
# MAGIC
# MAGIC Add this function to the shared config notebook (next to `saveTabletemp`).
# MAGIC It owns the whole "initial vs. delta" temp-write block that is currently
# MAGIC copy-pasted into notebooks 3124 / 3125, so each notebook just hands over the
# MAGIC full result DataFrame and a lookback window.
# MAGIC
# MAGIC The function itself:
# MAGIC   * decides initial vs. delta load by inspecting the temp folder,
# MAGIC   * computes `backfill_start` from a (number, unit) lookback,
# MAGIC   * on a delta load filters the DataFrame to that window and overwrites only
# MAGIC     those partitions via Delta `replaceWhere`.

# COMMAND ----------

import re
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, TimestampType

# inferPartitionDateFormat - Detect how a partition column stores its date value.
#
# Date / Timestamp columns are taken straight as 'yyyy-MM-dd'. For any other type
# (string, int, ...) the column rarely advertises its layout, so we sample one
# non-null value, cast it to string and match its shape against the common
# partition layouts. The returned pattern is a date_format pattern, so the same
# string used to render the cutoff also sorts chronologically as text -- which is
# what the `>=` filter and `replaceWhere` below rely on.
#
# Returns the date_format pattern, or None if the column is empty / unrecognised.
def inferPartitionDateFormat(df, partitionCol):
  dtype = df.schema[partitionCol].dataType
  if isinstance(dtype, (DateType, TimestampType)):
    return "yyyy-MM-dd"

  rows = (
    df.select(F.col(partitionCol).cast("string").alias("v"))
      .where(F.col("v").isNotNull())
      .limit(1)
      .collect()
  )
  if not rows or rows[0]["v"] is None:
    return None
  sample = rows[0]["v"].strip()

  # Ordered most-specific first so e.g. 'yyyy-MM-dd' wins before 'yyyy-MM'.
  patterns = [
    (r"^\d{4}-\d{2}-\d{2}$", "yyyy-MM-dd"),
    (r"^\d{4}/\d{2}/\d{2}$", "yyyy/MM/dd"),
    (r"^\d{4}-\d{2}$",       "yyyy-MM"),
    (r"^\d{4}/\d{2}$",       "yyyy/MM"),
    (r"^\d{8}$",             "yyyyMMdd"),
    (r"^\d{6}$",             "yyyyMM"),
    (r"^\d{4}$",             "yyyy"),
  ]
  for rx, fmt in patterns:
    if re.match(rx, sample):
      return fmt
  return None

# COMMAND ----------

# SaveTabletempPartitions - Save/overwrite a partitioned Delta table in pipeline-temp.
#
# Initial load (temp folder empty) -> (re)create the temp Delta table, partitioned
#   by `partitionCol`, writing the whole DataFrame (delegates to saveTabletemp).
# Delta load -> keep only partitions at/above `backfill_start` and overwrite just
#   those in place via Delta `replaceWhere`, leaving older partitions untouched.
#
# `backfill_start` is computed as exactly `lookback` `unit`s back from today:
#   D -> days, W -> weeks, M -> months, Y -> years,
#   FY -> fiscal year (July 1 - June 30); anchored to July 1 of the fiscal year
#         `lookback` years ago.
# It is then rendered to match how the partition column stores dates, detected
# automatically from the column type / a sample value (see inferPartitionDateFormat).
#
# Args:
#   tableName     Temp table name (folder under /mnt/mda-pipeline-temp/).
#   df            Full result DataFrame (the function filters the delta window itself).
#   partitionCol  Column to partition by and to filter on for delta loads.
#   lookback      Whole number of units to look back for the delta window.
#   unit          One of "D", "W", "M", "Y", "FY".
#
# Returns:
#   Number of rows written (0 when nothing was written).
def saveTabletempPartitions(tableName, df, partitionCol, lookback, unit):
  tempPath = "/mnt/mda-pipeline-temp/" + tableName

  # 1) Load type: initial when the temp folder has no Delta/parquet data yet.
  try:
    files = dbutils.fs.ls(tempPath)
    isInitialLoad = not any(f.name.endswith(".parquet") or "_delta_log" in f.name for f in files)
  except Exception:
    isInitialLoad = True

  if isInitialLoad:
    print("INITIAL LOAD — writing full dataset to temp")
    df_to_save = df
    backfillStart = None
  else:
    # 2) backfill_start = exactly `lookback` units back from today.
    unit = unit.upper()
    if unit == "D":
      startCol = F.date_sub(F.current_date(), lookback)
    elif unit == "W":
      startCol = F.date_sub(F.current_date(), lookback * 7)
    elif unit == "M":
      startCol = F.add_months(F.current_date(), -lookback)
    elif unit == "Y":
      startCol = F.add_months(F.current_date(), -12 * lookback)
    elif unit == "FY":
      # Fiscal year start year = current year if month >= July, else previous year.
      startCol = F.expr(
        "make_date("
        "CASE WHEN month(current_date()) >= 7 THEN year(current_date()) "
        "ELSE year(current_date()) - 1 END - " + str(lookback) + ", 7, 1)"
      )
    else:
      raise ValueError("Unknown unit '" + str(unit) + "'. Use one of: D, W, M, Y, FY.")

    # 3) Detect how the partition column stores dates and render the cutoff to match.
    fmt = inferPartitionDateFormat(df, partitionCol)
    if fmt is None:
      raise ValueError(
        f"Could not infer a date format for partition column '{partitionCol}'. "
        f"Make it a date/timestamp column or store dates as "
        f"yyyy-MM-dd / yyyy-MM / yyyyMMdd / yyyyMM / yyyy."
      )
    backfillStart = spark.range(1).select(F.date_format(startCol, fmt).alias("v")).first()["v"]

    print(f"DELTA LOAD — recomputing {partitionCol} >= {backfillStart}")
    df_to_save = df.filter(F.col(partitionCol) >= F.lit(backfillStart))

  newCount = df_to_save.count()
  print(f"Rows to write: {newCount}")

  if newCount == 0:
    print("No new records — skipping temp write")
    return 0

  if isInitialLoad:
    saveTabletemp(tableName, "delta", "overwrite", df_to_save, partitionCol)
    print(f"Temp Delta written (initial, partitioned by {partitionCol})")
  else:
    (
      df_to_save.write
      .format("delta")
      .option("replaceWhere", f"{partitionCol} >= '{backfillStart}'")
      .mode("overwrite")
      .save(tempPath)
    )
    print(f"Temp Delta written (replaceWhere {partitionCol} >= '{backfillStart}')")

  return newCount

# COMMAND ----------

# MAGIC %md
# MAGIC ## How notebooks 3124 / 3125 use it
# MAGIC
# MAGIC The whole load-type / backfill / count / write block collapses to one call.
# MAGIC `is_initial_load`, `path_has_data` and the delta-window filtering are no
# MAGIC longer needed in the notebook for this step.

# COMMAND ----------

# --- Notebook 3124 (snapshot_date, daily date column) ---
# Recompute the last 2 months of snapshots on a delta load:
#
# saveTabletempPartitions(
#     "supplier_fg_inv_vs_iwl_daily_summary_fact",
#     df_result,
#     "snapshot_date",
#     2, "M",
# )

# COMMAND ----------

# --- Notebook 3125 (asn_creation_month, yyyyMM column) ---
# Recompute the last 2 months on a delta load:
#
# saveTabletempPartitions(
#     "inbound_asn_psm_e2o_fact",
#     df_result,
#     "asn_creation_month",
#     2, "M",
# )
