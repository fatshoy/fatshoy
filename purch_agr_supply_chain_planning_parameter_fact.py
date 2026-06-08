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
# MAGIC Dedup key: `(material_id, site_code, case_id_format, slea.vendor_id)`.
# MAGIC When the same slea vendor is reachable via both a parent-case path (puma vendor NULL)
# MAGIC and a child-case path (puma vendor populated), the child-case row wins.
# MAGIC Distinct slea vendors for the same case are kept as separate output rows.
# MAGIC
# MAGIC `case_id_format` is computed only for joining/dedup (strips the `-V<n>` suffix);
# MAGIC the surviving row's original `case_id` (e.g. `PAPAC-031215-V1`) is what reaches the output.
# MAGIC
# MAGIC ### vendor_id in output
# MAGIC For child cases (`parent_case_flag = '0'`) the puma `vendor_id` is used as-is.
# MAGIC For parent cases (`parent_case_flag = '1'`) puma has no vendor; the slea `vendor_id` is
# MAGIC used instead (`COALESCE(puma.vendor_id, slea.vendor_id)`).
# MAGIC
# MAGIC ### psm_owner_email fallback
# MAGIC When puma's `psm_owner_email` is blank (NULL or empty), the slea `owner_email`
# MAGIC from the matched row is used as fallback. The slea row is already matched on
# MAGIC `(vendor_id, material_id, site_code, case_id_format)` by the main join, so no
# MAGIC additional lookup is required.
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
SOURCE_TABLE_SLEA = "sbm.slea_supply_chain_param_config_recom_fact"  # TODO: update to new source table
TARGET_TABLE_NAME = "purch_agr_supply_chain_planning_parameter_fact"
TARGET_PATH       = "/mnt/mda-pipeline-refined/purch_agr_supply_chain_planning_parameter_fact"

BUSINESS_KEY = ["material_id", "site_code", "case_id", "vendor_id"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Build joined and deduplicated dataset

# COMMAND ----------

SQL = f"""
WITH slea_dedup AS (
    -- Deduplicate slea by business key: content of all target columns is identical
    -- across duplicate rows; ORDER BY slea_ticket_id gives a deterministic tiebreaker.
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
        owner_email,
        -- Blank in any field propagates NULL to the whole PDT (0 is kept only when stored as 0).
        -- CASE guard needed because Spark LEAST() skips NULLs instead of propagating them.
        (slea_order_process_days
         + slea_transport_planning_days
         + slea_cover_unavailable_ship_days
         + slea_transit_days
         + slea_supplier_mps_zone_days
         + slea_customs_clearance_days)
        - CASE
            WHEN slea_supplier_goods_inventory_days IS NULL THEN NULL
            ELSE LEAST(slea_supplier_goods_inventory_days, slea_supplier_mps_zone_days)
          END                                         AS slea_reco_planned_deliv_time
    FROM {SOURCE_TABLE_SLEA}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY material_id, site_code, puma_case_id, vendor_id
        ORDER BY slea_ticket_id NULLS LAST
    ) = 1
),
puma_base AS (
    SELECT
        mapped_source_system_code,
        agreement_naturalkey                     AS agreement_natural_key,
        case_id,
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
    p.case_id,
    p.case_status,
    p.material_id,
    p.site_code,
    COALESCE(p.vendor_id, s.vendor_id) AS vendor_id,
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
    COALESCE(NULLIF(TRIM(p.psm_owner_email), ''), s.owner_email) AS psm_owner_email,
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
    -- One row per (case, slea-vendor): when the same slea vendor is reachable via both
    -- a parent-case path and a child-case path, keep the child-case row.
    PARTITION BY p.material_id, p.site_code, p.case_id_format, s.vendor_id
    ORDER BY
        CASE WHEN p.parent_case_flag = '0' THEN 0 ELSE 1 END
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
