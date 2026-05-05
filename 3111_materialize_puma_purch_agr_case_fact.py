# Databricks notebook source
# MAGIC %md
# MAGIC ### Notebook: 3111_materialize_puma_purch_agr_case_fact
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Business Context
# MAGIC Materializes PUMA purchasing agreement case details by mapping regional source system codes and joining with planning parameter facts to enrich case records with agreement natural keys.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Source Tables
# MAGIC | Source | Table | Mount / Path |
# MAGIC |--------|-------|-------------|
# MAGIC | EMDL | EMDL_puma_initiative_case_details | /mnt/mda-raw-emdl/EMDL_puma_initiative_case_details |
# MAGIC | EMDL | EMDL_puma_initiative_steps | /mnt/mda-raw-emdl/EMDL_puma_initiative_steps |
# MAGIC | Supply Chain (Refined) | planning_parameter_fact | /mnt/mda-pipeline-refined/planning_parameter_fact |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Developers Involved (Git History)
# MAGIC - gopoj
# MAGIC - krezolekm
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Additional Notes
# MAGIC
# MAGIC **Filters:**
# MAGIC - case_id segments IN (2,3); puma_case_status != 'Resolved-Cancelled'
# MAGIC
# MAGIC **Logic & Calculations:**
# MAGIC - Maps puma_region to source_system_code; lpad on vendor/plant/material IDs
# MAGIC - parent_case_flag derived from case_id segment count
# MAGIC - puma_pp_steps: temporary view from emdl_puma_initiative_steps filtered to puma_step_name = 'Planning Parameters', joined by puma_case_id

# COMMAND ----------

# MAGIC %run "../../utilities/DQConfig"

# COMMAND ----------

emdl_puma_initiative_case_details = spark.read.parquet("/mnt/mda-raw-emdl/EMDL_puma_initiative_case_details")
emdl_puma_initiative_case_details.createOrReplaceTempView("emdl_puma_initiative_case_details")

emdl_puma_initiative_steps = spark.read.parquet("/mnt/mda-raw-emdl/EMDL_puma_initiative_steps")
emdl_puma_initiative_steps.createOrReplaceTempView("emdl_puma_initiative_steps")

planning_parameter_fact = spark.read.parquet('/mnt/mda-pipeline-refined/planning_parameter_fact')
planning_parameter_fact.createOrReplaceTempView("planning_parameter_fact")

# COMMAND ----------

sql_str = """
CREATE OR REPLACE TEMPORARY VIEW puma_pp_steps AS
SELECT *
FROM emdl_puma_initiative_steps
WHERE puma_step_name = 'Planning Parameters'
"""
spark.sql(sql_str)

# COMMAND ----------

sql_str = """
SELECT
    MAX(agreement_naturalkey) as agreement_naturalkey,
    source_system_code,
    purchase_vendor_id,
    plant_code,
    material_id
FROM planning_parameter_fact
GROUP BY source_system_code, purchase_vendor_id, plant_code, material_id
"""
ppf = spark.sql(sql_str)
ppf.createOrReplaceTempView("ppf")

# COMMAND ----------

sql_str = """
SELECT
  CASE
    WHEN cases.puma_region = 'LA' THEN 'L6P430'
    WHEN cases.puma_region IN ('MEA', 'RU', 'EU') THEN 'F6PS4H'
    WHEN cases.puma_region IN ('AMAE', 'APAC', 'GC', 'INDIA') THEN 'A6PS4H'
    WHEN cases.puma_region = 'NA' THEN 'N6P420'
  END AS mapped_source_system_code,
  cases.puma_case_id AS case_id,
  cases.puma_case_status AS case_status,
  cases.puma_initiative_num_id AS iopt_initiative_id,
  lpad(cases.puma_material_id, 18, '0') AS material_id,
  cases.puma_material_description AS material_desc,
  cases.puma_material_group_description AS material_group_desc,
  lpad(cases.puma_plant_code, 4, '0') AS site_code,
  cases.puma_plant_name AS site_name,
  cases.puma_plant_geography_key AS site_country_code,
  lpad(cases.puma_vendor_code, 10, '0') AS vendor_id,
  cases.puma_vendor_name AS vendor_name,
  cases.puma_scenario AS scenario,
  cases.puma_volume AS vol,
  cases.puma_artwork_project_type AS artwork_project_type,
  cases.puma_manufacturer_supplier_equivalent_part_list AS mep_sep_list,
  cases.puma_document_number AS purch_doc_num,
  cases.puma_reference_code AS ref_code,
  cases.puma_phase_out_code AS phase_out_code,
  cases.puma_initiative_name AS initiative_name,
  cases.puma_urgent_flag AS urgent_flag,
  cases.puma_base_unit_of_measure AS base_uom,
  cases.puma_requestor_id AS requestor_id,
  CAST(NULL AS STRING) AS loc_time_zone,
  cases.puma_puma_target_date AS puma_trgt_date,
  cases.puma_external_supply_solutions_type AS ess_type,
  cases.puma_planned_delivery_time AS pdt_days,
  cases.puma_firm_zone AS firm_zone_code,
  cases.puma_trade_off_zone AS trade_off_zone_code,
  cases.puma_goods_receipt_processing_time AS gr_process_days,
  cases.puma_minimal_loading_on_time_size AS min_lot_size,
  cases.puma_rounding_value AS round_value,
  cases.puma_material_origin_id AS material_origin_id,
  cases.puma_material_usage_id AS material_usage_id,
  cases.puma_loading_efficiency AS load_efficiency,
  CAST(cases.puma_work_days_overdue AS INT) AS work_days_overdue,
  steps.puma_off_track_reason AS off_track_reason,
  steps.puma_comments AS comments,
  CASE WHEN cases.puma_region = 'PEU' THEN steps.puma_step_completed_by ELSE NULL END AS psm_owner_email,
  CASE WHEN cases.puma_region != 'PEU' THEN steps.puma_step_completed_by ELSE NULL END AS buyer_owner_email,
  SUBSTRING(REPLACE(cases.create_date, '-', ''), 1, 8) AS create_date,
  SUBSTRING(REPLACE(cases.update_date, '-', ''), 1, 8) AS update_date,
  ppf.agreement_naturalkey,
  CAST(CASE WHEN ARRAY_SIZE(SPLIT(cases.puma_case_id, '-')) = 2 THEN 1 ELSE 0 END AS STRING) AS parent_case_flag
FROM emdl_puma_initiative_case_details cases
LEFT JOIN puma_pp_steps steps
  ON steps.puma_case_id = cases.puma_case_id
LEFT JOIN ppf
  ON CASE
       WHEN cases.puma_region = 'LA' THEN 'L6P430'
       WHEN cases.puma_region IN ('MEA', 'RU', 'EU') THEN 'F6PS4H'
       WHEN cases.puma_region IN ('AMAE', 'APAC', 'GC', 'INDIA') THEN 'A6PS4H'
       WHEN cases.puma_region = 'NA' THEN 'N6P420'
     END = ppf.source_system_code
  AND lpad(cases.puma_vendor_code, 10, '0') = ppf.purchase_vendor_id
  AND lpad(cases.puma_plant_code, 4, '0') = ppf.plant_code
  AND lpad(cases.puma_material_id, 18, '0') = ppf.material_id
WHERE array_size(SPLIT(cases.puma_case_id, '-')) IN (2, 3)
  AND cases.puma_case_status != 'Resolved-Cancelled'
"""
puma_purch_agr_case_fact = spark.sql(sql_str)

# COMMAND ----------

saveTabletemp('puma_purch_agr_case_fact', 'parquet', 'overwrite', puma_purch_agr_case_fact)

# COMMAND ----------

puma_purch_agr_case_fact_temp = spark.read.parquet("/mnt/mda-pipeline-temp/puma_purch_agr_case_fact")
expect_table_records_to_be_unique(puma_purch_agr_case_fact_temp, 'puma_purch_agr_case_fact', False, ['case_id'], False)
if result == 'Success':
  saveTable('puma_purch_agr_case_fact', 'parquet', 'overwrite', puma_purch_agr_case_fact_temp)

# COMMAND ----------

dbutils.notebook.exit(notebookReturnSuccess)
