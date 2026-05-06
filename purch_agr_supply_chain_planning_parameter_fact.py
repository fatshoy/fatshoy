# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Purch Agr Supply Chain Planning Parameter Fact
# MAGIC
# MAGIC Materializes `rpm.purch_agr_supply_chain_planning_parameter_fact` by joining:
# MAGIC - **Left / driving table**: `rpm.puma_purch_agr_case_fact`
# MAGIC - **Right**: `sbm.slea_supply_chain_param_config_recom_fact`
# MAGIC
# MAGIC ### Join strategy (driven by `parent_case_flag`)
# MAGIC | `parent_case_flag` (string) | Join keys |
# MAGIC |---|---|
# MAGIC | `'1'` — parent case (vendor is NULL) | `material_id + site_code + case_id_format` |
# MAGIC | `'0'` — child case (vendor populated) | `vendor_id + material_id + site_code + case_id_format` |
# MAGIC
# MAGIC ### Post-join deduplication
# MAGIC When the same `(material_id, site_code, case_id_format)` exists in puma with both a
# MAGIC NULL/blank `vendor_id` and a populated one, the NULL/blank entry is dropped.
# MAGIC
# MAGIC ### slea pre-deduplication
# MAGIC slea contains rows that are content-identical but differ by `vendor_source` (SEP vs MEP).
# MAGIC A `ROW_NUMBER` keyed on `(material_id, site_code, puma_case_id, vendor_id)` collapses
# MAGIC these before the join to prevent row multiplication in puma.
# MAGIC
# MAGIC **Load strategy**: full overwrite on every run.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

SOURCE_TABLE_PUMA = "rpm.puma_purch_agr_case_fact"
SOURCE_TABLE_SLEA = "sbm.slea_supply_chain_param_config_recom_fact"
TARGET_TABLE_NAME = "purch_agr_supply_chain_planning_parameter_fact"
TARGET_PATH       = "/mnt/mda-pipeline-refined/purch_agr_supply_chain_planning_parameter_fact"

BUSINESS_KEY = ["material_id", "site_code", "case_id_format"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Build joined and deduplicated dataset

# COMMAND ----------

SQL = f"""
WITH slea_dedup AS (
    -- Collapse SEP/MEP duplicates: content of all target columns is identical across
    -- vendor_source values; ORDER BY vendor_source gives a deterministic tiebreaker.
    -- slea_reco_planned_deliv_time is a SQL translation of the DAX formula:
    --   COALESCE applied to every addend to replicate DAX BLANK-as-zero arithmetic.
    SELECT
        material_id,
        site_code,
        puma_case_id,
        vendor_id,
        po_firm_zone_days,
        po_trade_off_zone_days,
        po_gr_process_days,
        mm_min_lot_size,
        mm_rounding_value,
        slea_material_usage_id_desc,
        slea_material_origin_id_desc,
        calc_loading_efficiency,
        concat_email_vendor,
        (COALESCE(slea_order_process_days,             0)
         + COALESCE(slea_transport_planning_days,      0)
         + COALESCE(slea_cover_unavailable_ship_days,  0)
         + COALESCE(slea_transit_days,                 0)
         + COALESCE(slea_supplier_mps_zone_days,       0)
         + COALESCE(slea_customs_clearance_days,       0))
        - LEAST(
            COALESCE(slea_supplier_goods_inventory_days, 0),
            COALESCE(slea_supplier_mps_zone_days,        0)
          )                                           AS slea_reco_planned_deliv_time
    FROM {SOURCE_TABLE_SLEA}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY material_id, site_code, puma_case_id, vendor_id
        ORDER BY vendor_source
    ) = 1
),
puma_base AS (
    SELECT
        mapped_source_system_code,
        agreement_naturalkey                     AS agreement_natural_key,
        regexp_replace(case_id, '-V[0-9]+$', '') AS case_id_format,
        case_status,
        parent_case_flag,
        material_id,
        site_code,
        vendor_id,
        scenario,
        vol                                      AS vol_num_format,
        artwork_project_type,
        mep_sep_list,
        purch_doc_num,
        ref_code,
        phase_out_code,
        initiative_name,
        urgent_flag,
        base_uom,
        requestor_id,
        loc_time_zone,
        puma_trgt_date,
        create_date,
        update_date,
        work_days_overdue,
        off_track_reason,
        comments,
        psm_owner_email,
        ess_type,
        site_country_code
    FROM {SOURCE_TABLE_PUMA}
)
SELECT
    p.mapped_source_system_code,
    p.agreement_natural_key,
    p.case_id_format,
    p.case_status,
    p.material_id,
    p.site_code,
    p.vendor_id,
    p.scenario,
    p.vol_num_format,
    p.artwork_project_type,
    p.mep_sep_list,
    p.purch_doc_num,
    p.ref_code,
    p.phase_out_code,
    p.initiative_name,
    p.urgent_flag,
    p.base_uom,
    p.requestor_id,
    p.loc_time_zone,
    p.puma_trgt_date,
    p.create_date,
    p.update_date,
    p.work_days_overdue,
    p.off_track_reason,
    p.comments,
    p.psm_owner_email,
    p.ess_type,
    p.site_country_code,
    s.slea_reco_planned_deliv_time,
    s.po_firm_zone_days            AS slea_reco_po_firm_zone_days,
    s.po_trade_off_zone_days       AS slea_reco_po_trade_off_zone_days,
    s.po_gr_process_days           AS slea_reco_po_gr_process_days,
    s.mm_min_lot_size              AS slea_reco_mm_min_lot_size,
    s.mm_rounding_value            AS slea_reco_mm_rounding_value,
    s.slea_material_usage_id_desc  AS slea_reco_material_usage_id_desc,
    s.slea_material_origin_id_desc AS slea_reco_material_origin_id_desc,
    s.calc_loading_efficiency      AS slea_reco_calc_loading_efficiency,
    s.concat_email_vendor          AS slea_concat_email_vendor
FROM puma_base p
LEFT JOIN slea_dedup s
    ON  s.material_id  = p.material_id
    AND s.site_code    = p.site_code
    AND s.puma_case_id = p.case_id_format
    AND (
        p.parent_case_flag = '1'
        OR (p.parent_case_flag = '0' AND p.vendor_id = s.vendor_id)
    )
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY p.material_id, p.site_code, p.case_id_format
    ORDER BY
        CASE WHEN p.vendor_id IS NULL OR TRIM(p.vendor_id) = '' THEN 1 ELSE 0 END
) = 1
"""

df_result = spark.sql(SQL).cache()
print(f"Records after join and dedup: {df_result.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: DQ check

# COMMAND ----------

dq_result = expect_table_records_to_be_unique(df_result, TARGET_TABLE_NAME, False, BUSINESS_KEY, False)
print(f"DQ result: {dq_result}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Save to SA (parquet, overwrite)

# COMMAND ----------

if dq_result == 'Success':
    saveTable(TARGET_TABLE_NAME, 'parquet', 'overwrite', df_result)
else:
    raise Exception(f"DQ check failed for {TARGET_TABLE_NAME}. Load aborted.")
