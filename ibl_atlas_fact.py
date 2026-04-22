# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # IBL Atlas Fact
# MAGIC
# MAGIC Builds `ibl_atlas_fact` from `netsavings_prod_gold_fdd_b4` enriched with
# MAGIC geo / product / spend-pool hierarchies and several code-description lookups.
# MAGIC Mapping follows the STTM (Subject Area RPM / IBL).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Register spend-pool hierarchy CSV as a temp view

# COMMAND ----------

spark.read.csv(
    "/mnt/mda-pipeline-raw/fps/spend_pool_hierarchy_dap.csv", header=True
).createOrReplaceTempView("spl_hier_dap")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Build the fact dataframe
# MAGIC
# MAGIC `base` CTE materialises all joined source columns plus `ta_cur`
# MAGIC (time-adjusted current), which is reused for `tapa_cur` and `carry_over`
# MAGIC to avoid recomputing the guarded division.

# COMMAND ----------

sql = """
WITH base AS (
    SELECT
        ns.ProjectID,
        ns.ProjectName,
        ns.ProjectClassification,
        ns.totalsavings,
        ns.TotalNumberofMonths,
        ns.MonthsinFiscalYear,
        ns.Probability,
        ns.FiscalYear,
        ns.E2E_StartDate,
        ns.E2E_EndDate,
        ns.SavingsCategoryName,
        ns.PurchasingGroup,
        ns.ppvflag,
        ns.NSFlag,
        ns.ProjectType,
        ns.UpdatedForecastCycle,
        ns.Feedstock,
        ns.ProjectNotes,
        ns.TechnicalResourcesRequiredFlag,
        ns.OneTimeSavings,
        ns.SubSector,
        ns.SubRegion,
        ns.SpendPool_Low,
        ns.StandardProject,
        ns.FAEntryTypeID,
        ns.GCASChange,
        ns.TNumber,
        r.level_02_short_txt,
        r.level_03_short_txt,
        s.subsector_short_name,
        spl.spend_pool_low_name,
        spl.spend_pool_med_name,
        spl.spend_pool_high_name,
        pc.projectclassificationdescription,
        ph.description          AS procurement_handling_desc,
        fet.LongDescription     AS faentry_longdesc,
        gc.LongDescription      AS gcas_longdesc,
        pd.common_name          AS buyer_common_name,
        -- Time-Adjusted Current:
        --   diff = TotalNumberofMonths - MonthsinFiscalYear
        --   if diff == 0 -> totalsavings
        --   else -> (totalsavings / TotalNumberofMonths) * diff
        -- Guarded against TotalNumberofMonths = 0 / NULL.
        CAST(
            CASE
                WHEN (ns.TotalNumberofMonths - ns.MonthsinFiscalYear) = 0
                    THEN ns.totalsavings
                WHEN COALESCE(ns.TotalNumberofMonths, 0) = 0
                    THEN NULL
                ELSE (ns.totalsavings / ns.TotalNumberofMonths)
                     * (ns.TotalNumberofMonths - ns.MonthsinFiscalYear)
            END AS decimal(18,6)
        ) AS ta_cur
    FROM app_fps_prod.inbound_supply_chain_shares.netsavings_prod_gold_fdd_b4 ns
    LEFT JOIN (
        SELECT DISTINCT level_03, level_02_short_txt, level_03_short_txt
        FROM cdl_fps_prod.silver_master_data.geo_705_flat_hier_dim
        WHERE valid_to = '99991231'
    ) r ON ns.SubRegion = r.level_03
    LEFT JOIN (
        SELECT DISTINCT subsector_legacy_id, subsector_short_name
        FROM cdl_fps_prod.silver_master_data.mgmt_product_dim
    ) s ON ns.SubSector = s.subsector_legacy_id
    LEFT JOIN (
        SELECT DISTINCT
            spend_pool_low_code,
            spend_pool_low_name,
            spend_pool_medium_name AS spend_pool_med_name,
            spend_pool_high_name
        FROM spl_hier_dap
    ) spl ON ns.SpendPool_Low = spl.spend_pool_low_code
    LEFT JOIN pp_fps_prod.inbound_supply_chain_shares.netsavings_prod_bronze_projectclassification pc
        ON ns.ProjectClassification = pc.idx
    LEFT JOIN pp_fps_prod.inbound_supply_chain_shares.netsavings_prod_bronze_procurementhandling ph
        ON ns.StandardProject = ph.idx
    LEFT JOIN pp_fps_prod.inbound_supply_chain_shares.netsavings_prod_gold_faentrytype fet
        ON ns.FAEntryTypeID = fet.FAEntryTypeID
    LEFT JOIN pp_fps_prod.inbound_supply_chain_shares.netsavings_prod_gold_gcaschange gc
        ON ns.GCASChange = gc.GCASChangeID
    LEFT JOIN (
        SELECT DISTINCT tnumber, common_name
        FROM cdl_fps_prod.silver_master_data.people_dim
    ) pd ON ns.TNumber = pd.tnumber
)
SELECT
    CASE
        WHEN level_02_short_txt IN ('EUROPE FOCUS', 'EUROPE EM') THEN 'EUROPE'
        WHEN level_02_short_txt = 'NORTH AMERICA'                THEN 'NA'
        WHEN level_02_short_txt = 'LATIN AMERICA'                THEN 'LA'
        WHEN level_02_short_txt = 'GREATER CHINA'                THEN 'GC'
        WHEN level_02_short_txt = 'AMA'                          THEN 'AMA-W'
        WHEN level_02_short_txt = 'APAC FM'                      THEN 'AMA-E'
        ELSE 'missing'
    END                                                          AS region_name,
    level_03_short_txt                                           AS subregion_name,
    subsector_short_name                                         AS business_unit,
    spend_pool_low_name,
    spend_pool_med_name,
    spend_pool_high_name,
    CASE
        WHEN projectclassificationdescription = 'MLC'
            THEN 'MLC Project Negotiation (F&A)'
        WHEN LOWER(ProjectName) RLIKE
             '(export|freight|transport|truck|storage|tank|import|duty|logistics|pallet|loading|mlc|container|railcar|barge|frt|ibl|isotainer|unloading|isotank)'
            THEN 'MLC Project Negotiation (ATLAS)'
        WHEN LOWER(ProjectName) RLIKE '(incoterm|localization)'
            THEN 'MLC Project Negotiation (Manual)'
        ELSE NULL
    END                                                          AS mlc_project_clasification,
    projectclassificationdescription                             AS project_clasification,
    buyer_common_name                                            AS buyer_name,
    procurement_handling_desc                                    AS procurement_handling,
    faentry_longdesc                                             AS fna_entry_type,
    gcas_longdesc                                                AS material_change,
    ProjectID                                                    AS project_id,
    ProjectName                                                  AS project_name,
    CAST(totalsavings AS decimal(18,6))                          AS project_val,
    ta_cur,
    CAST(ta_cur * (Probability / 100.0) AS decimal(18,6))        AS tapa_cur,
    CAST(totalsavings - ta_cur      AS decimal(18,6))            AS carry_over,
    CAST(Probability               AS decimal(5,2))              AS probability,
    FiscalYear                                                   AS fiscal_year_short_text,
    TotalNumberofMonths                                          AS total_num_mths,
    MonthsinFiscalYear                                           AS mth_in_current_fy,
    date_format(E2E_StartDate, 'yyyyMMdd')                       AS start_date,
    date_format(E2E_EndDate,   'yyyyMMdd')                       AS end_date,
    SavingsCategoryName                                          AS category,
    PurchasingGroup                                              AS purchase_group_id,
    ppvflag                                                      AS ppv_flag,
    NSFlag                                                       AS ns_flag,
    ProjectType                                                  AS forecast_type,
    UpdatedForecastCycle                                         AS forecast_cycle,
    Feedstock                                                    AS commodity_name,
    ProjectNotes                                                 AS flex_field,
    ProjectNotes                                                 AS additional_notes,
    CAST(TechnicalResourcesRequiredFlag AS string)               AS technical_resources_required_flag,
    CAST(OneTimeSavings AS string)                               AS one_time_savings
FROM base
"""

df_final = spark.sql(sql)
