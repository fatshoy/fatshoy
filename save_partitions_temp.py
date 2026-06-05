# Databricks notebook source

# MAGIC %md
# MAGIC # Config helper: saveTabletempPartitions
# MAGIC
# MAGIC Add this function to the shared config notebook (next to `saveTabletemp`).
# MAGIC It wraps the "initial vs. delta" temp-write block that is currently copy-pasted
# MAGIC into notebooks 3124 and 3125 so each notebook only needs a single call.
# MAGIC
# MAGIC The notebook still computes `is_initial_load` and `df_to_save` itself; this
# MAGIC helper just decides between a full (re)write and a partition overwrite.

# COMMAND ----------

# SaveTabletempPartitions - Save/overwrite a partitioned Delta table in pipeline-temp.
#
# - Initial load  -> (re)create the temp Delta table, partitioned by `partitionCol`,
#                    writing the whole DataFrame (delegates to saveTabletemp).
# - Delta load    -> overwrite only the partitions at/above `backfillStart` in place
#                    via Delta `replaceWhere`, leaving older partitions untouched.
#
# Rows are counted first; nothing is written when the DataFrame is empty.
#
# Args:
#   tableName       Temp table name (folder under /mnt/mda-pipeline-temp/).
#   df              DataFrame to write.
#   partitionCol    Column to partition by and to filter on for delta loads.
#   isInitialLoad   True to (re)write the whole table, False for a partition overwrite.
#   backfillStart   Inclusive lower bound of the partition values to overwrite on a
#                   delta load (e.g. a date '2026-06-03' or a 'yyyyMM' string '202604').
#                   Ignored on the initial load.
#
# Returns:
#   Number of rows written (0 when nothing was written).
def saveTabletempPartitions(tableName, df, partitionCol, isInitialLoad, backfillStart=""):
  newCount = df.count()
  print(f"Rows to write: {newCount}")

  if newCount == 0:
    print("No new records — skipping temp write")
    return 0

  if isInitialLoad:
    saveTabletemp(tableName, "delta", "overwrite", df, partitionCol)
    print(f"Temp Delta written (initial, partitioned by {partitionCol})")
  else:
    (
      df.write
      .format("delta")
      .option("replaceWhere", f"{partitionCol} >= '{backfillStart}'")
      .mode("overwrite")
      .save("/mnt/mda-pipeline-temp/" + tableName)
    )
    print(f"Temp Delta written (replaceWhere {partitionCol} >= '{backfillStart}')")

  return newCount

# COMMAND ----------

# MAGIC %md
# MAGIC ## How notebooks 3124 / 3125 use it
# MAGIC
# MAGIC `is_initial_load` and `df_to_save` stay in the notebook; the whole
# MAGIC count + branch + write block becomes one call.

# COMMAND ----------

# --- Notebook 3124 (snapshot_date) ---
#
# PARTITION_COL = "snapshot_date"
# is_initial_load = not path_has_data(TEMP_PATH)
#
# if is_initial_load:
#     print(f"INITIAL LOAD — writing full window ({cutoff_date} < snapshot_date) to temp")
#     df_to_save = df_result
# else:
#     print(f"DELTA LOAD — recomputing snapshot dates >= {backfill_start}")
#     df_to_save = spark.sql(f"""
#         SELECT * FROM v_result WHERE snapshot_date >= DATE '{backfill_start}'
#     """)
#
# saveTabletempPartitions(
#     "supplier_fg_inv_vs_iwl_daily_summary_fact",
#     df_to_save, PARTITION_COL, is_initial_load, backfill_start,
# )

# COMMAND ----------

# --- Notebook 3125 (asn_creation_month) ---
#
# PARTITION_COL = "asn_creation_month"
# is_initial_load = not path_has_data("/mnt/mda-pipeline-temp/inbound_asn_psm_e2o_fact")
#
# if is_initial_load:
#     print("INITIAL LOAD — writing full history to temp")
#     df_to_save = df_result
# else:
#     print(f"DELTA LOAD — recomputing {PARTITION_COL} >= {backfill_start_month}")
#     df_to_save = spark.sql(f"""
#         SELECT * FROM v_result WHERE {PARTITION_COL} >= '{backfill_start_month}'
#     """)
#
# saveTabletempPartitions(
#     "inbound_asn_psm_e2o_fact",
#     df_to_save, PARTITION_COL, is_initial_load, backfill_start_month,
# )
