# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Inbound ASN PSM / e2o Fact
# MAGIC
# MAGIC Materializes an enriched ASN-level inbound delivery fact joined with
# MAGIC scheduling agreement, business unit, supply chain owner (PSM), supplier
# MAGIC attributes and the e2o active flag.
# MAGIC
# MAGIC - **Initial run** (no temp data yet): full history is loaded without any
# MAGIC   date predicate and written partitioned by `asn_creation_month`.
# MAGIC - **Weekly run** (temp data exists): only the last 2 calendar months are
# MAGIC   recomputed and the matching partitions are overwritten via
# MAGIC   `replaceWhere`; older partitions are left untouched.
# MAGIC
# MAGIC Final SA copy is written as parquet via the framework `saveTable` helper.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

TARGET_TABLE_NAME = "inbound_asn_psm_e2o_fact"
TEMP_PATH         = "/mnt/mda-pipeline-temp/inbound_asn_psm_e2o_fact"
PARTITION_COL     = "asn_creation_month"

# Source parquet paths -> temp view names referenced inside the SQL below.
# `asf` is not used by the current SELECT but is registered for parity with
# the upstream loader block.
SOURCE_PATHS = {
    "inb":          "/mnt/mda-pipeline-refined/inb_deliv_bol_agg_month_fact",
    "aaf":          "/mnt/mda-pipeline-refined/active_agreement_fact",
    "sc_owner":     "/mnt/mda-pipeline-refined/supply_chain_owner",
    "asf":          "/mnt/mda-pipeline-refined/active_supply_chain_fact",
    "bu_comb_dim":  "/mnt/mda-pipeline-refined/bu_comb_dim",
    "supplier_dim": "/mnt/mda-pipeline-refined/supplier_attr_dim",
    "e2o_active":   "/mnt/mda-pipeline-refined/e2o_active_supply_chain_dim",
}

# Business key for DQ uniqueness check on the materialized fact.
BUSINESS_KEY = [
    "delivery_document_number",
    "bill_of_lading",
    "Customer_Item_ID",
    "Customer_Site",
    "Supplier_ID",
    "Schedule_Agreement_ID",
    "Schedule_Agreement_Line_ID",
    "PSM",
    "asn_creation_date",
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
# MAGIC ## Step 2: Build full result (no date predicate)

# COMMAND ----------

query_full = """
SELECT
    inb.delivery_document_number,
    inb.bill_of_lading,
    inb.platform,
    left(inb.source_system_code, 3)   AS Customer_ID,
    CAST(inb.material_id AS BIGINT)   AS Customer_Item_ID,
    inb.vendor_id                     AS Supplier_ID,
    sd.vendor_name                    AS Supplier_Description,
    inb.plant_code                    AS Customer_Site,
    aaf.purchase_doc_num              AS Schedule_Agreement_ID,
    aaf.purchase_doc_line_num         AS Schedule_Agreement_Line_ID,
    aaf.mrp_type,
    aaf.source_list_flag,
    aaf.source_list_usage_flag,
    aaf.valid_to_date,
    bu.business_unit,
    sc_owner.email_owner              AS PSM,
    inb.asn_creation_date,
    CASE
        WHEN ea.vendor_id IS NOT NULL AND ea.plant_code IS NOT NULL THEN 'Y'
        ELSE 'N'
    END                               AS e2o_active_flag,
    substring(inb.asn_creation_date, 1, 6) AS asn_creation_month
FROM inb
LEFT JOIN aaf
    ON inb.vendor_id   = aaf.purchase_vendor_id
   AND inb.plant_code  = aaf.plant_code
   AND inb.material_id = aaf.material_id
LEFT JOIN bu_comb_dim bu
    ON bu.code = inb.business_unit_lkp_code
LEFT JOIN sc_owner
    ON inb.vendor_id    = sc_owner.vendor_id
   AND inb.plant_code   = sc_owner.plant_code
   AND bu.business_unit = sc_owner.business_unit
LEFT JOIN supplier_dim sd
    ON sd.vendor_id = inb.vendor_id
LEFT JOIN e2o_active ea
    ON inb.vendor_id  = ea.vendor_id
   AND inb.plant_code = ea.plant_code
"""

df_result = spark.sql(query_full)
df_result.createOrReplaceTempView("v_result")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Determine load type and build delta

# COMMAND ----------

def path_has_data(path: str) -> bool:
    try:
        files = dbutils.fs.ls(path)
        return any(f.name.endswith(".parquet") or "_delta_log" in f.name for f in files)
    except Exception:
        return False


is_initial_load = not path_has_data(TEMP_PATH)

# Start of month 2 months ago, formatted as yyyyMM to match PARTITION_COL.
backfill_start_month = spark.sql(
    "SELECT date_format(trunc(add_months(current_date(), -2), 'MM'), 'yyyyMM') AS m"
).first()["m"]

if is_initial_load:
    print("INITIAL LOAD — writing full history to temp")
    df_to_save = df_result
else:
    print(f"DELTA LOAD — recomputing {PARTITION_COL} >= {backfill_start_month}")
    df_to_save = spark.sql(f"""
        SELECT *
        FROM v_result
        WHERE {PARTITION_COL} >= '{backfill_start_month}'
    """)

new_count = df_to_save.count()
print(f"Rows to write: {new_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Write to temp (Delta, partitioned)

# COMMAND ----------

if new_count > 0:
    if is_initial_load:
        saveTabletemp(TARGET_TABLE_NAME, "delta", "overwrite", df_to_save, PARTITION_COL)
        print(f"Temp Delta written (initial, partitioned by {PARTITION_COL})")
    else:
        (
            df_to_save.write
            .format("delta")
            .option("replaceWhere", f"{PARTITION_COL} >= '{backfill_start_month}'")
            .mode("overwrite")
            .save(TEMP_PATH)
        )
        print(f"Temp Delta written (replaceWhere {PARTITION_COL} >= '{backfill_start_month}')")
else:
    print("No new records — skipping temp write")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: DQ check and save to SA (parquet, overwrite)

# COMMAND ----------

df_temp = spark.read.format("delta").load(TEMP_PATH)

result = expect_table_records_to_be_unique(
    df_temp, TARGET_TABLE_NAME, False, BUSINESS_KEY, False
)
if result == "Success":
    saveTable(TARGET_TABLE_NAME, "parquet", "overwrite", df_temp)
