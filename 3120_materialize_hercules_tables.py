# Databricks notebook source
# MAGIC %md
# MAGIC ### Notebook: 3120_materialize_hercules_tables
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Business Context
# MAGIC Materializes Hercules procurement savings data - produces both a subsector-level savings fact and an IBL ATLAS project-level fact from ATLAS/FPS net-savings tables.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Source Tables
# MAGIC | Source | Table | Mount / Path |
# MAGIC |--------|-------|-------------|
# MAGIC | Raw | spend_pool_hierarchy_dap.csv | /mnt/mda-pipeline-raw/fps/spend_pool_hierarchy_dap.csv |
# MAGIC | Supply Chain (Refined) | people_dim | /mnt/mda-pipeline-refined/people_dim |
# MAGIC | Unity Catalog (FPS) | netsavings_prod_gold_fdd_b4 | app_fps_prod.inbound_supply_chain_shares.netsavings_prod_gold_fdd_b4 |
# MAGIC | Unity Catalog (FPS) | geo_705_flat_hier_dim | cdl_fps_prod.silver_master_data.geo_705_flat_hier_dim |
# MAGIC | Unity Catalog (FPS) | mgmt_product_dim | cdl_fps_prod.silver_master_data.mgmt_product_dim |
# MAGIC
# MAGIC #### Additional Notes
# MAGIC
# MAGIC **Logic & Calculations:**
# MAGIC - Produces hercules_subsector_savings_fact and ibl_atlas_fact
# MAGIC - MLC classification via regex on project name keywords (freight, transport, logistics)
# MAGIC - ta_cur = (totalsavings / TotalNumberofMonths) * MonthsinFiscalYear
# MAGIC - hercules_subsector_savings_fact: sub-region K3 "(I)EUROPE EM" was end-dated in geo_705 on 2026-06-30 and replaced by XH "EUROPE AREA"; K3 savings lines are remapped to XH before the region join
# MAGIC - ibl_atlas_fact: K3 keeps its own sub-region "(I)EUROPE EM" (region EUROPE), since remapping to XH would duplicate the business key
# MAGIC

# COMMAND ----------

# MAGIC %run "../../utilities/DQConfig"

# COMMAND ----------

# DBTITLE 1,Read sources
spark.read.csv("/mnt/mda-pipeline-raw/fps/spend_pool_hierarchy_dap.csv", header=True).createOrReplaceTempView("spl_hier_dap")
spark.read.parquet('/mnt/mda-pipeline-refined/people_dim').createOrReplaceTempView('people_dim')

# COMMAND ----------

# DBTITLE 1,SQL transformation - ibl_atlas_fact
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
        r.region_name,
        r.level_03,
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
        CAST(
            CASE
                WHEN (ns.TotalNumberofMonths - ns.MonthsinFiscalYear) = 0
                    THEN ns.totalsavings
                WHEN COALESCE(ns.TotalNumberofMonths, 0) = 0
                    THEN NULL
                ELSE (ns.totalsavings / ns.TotalNumberofMonths)
                     * ns.MonthsinFiscalYear
            END AS decimal(18,6)
        ) AS ta_cur
    FROM app_fps_prod.inbound_supply_chain_shares.netsavings_prod_gold_fdd_b4 ns
    LEFT JOIN (
        SELECT DISTINCT
        CASE
            WHEN level_02_short_txt LIKE '%APAC%' THEN 'APAC'
            WHEN level_02_short_txt LIKE '%EUROPE%' THEN 'EUROPE'
            WHEN level_02_short_txt LIKE '%EECAR%' THEN 'EASTERN EUROPE'
            WHEN level_02_short_txt LIKE 'NORTH AMERICA' THEN 'NA'
            WHEN level_02_short_txt LIKE 'LATIN AMERICA' THEN 'LA'
            WHEN level_02_short_txt LIKE 'IMETA' THEN 'IMETA'
            WHEN level_02_short_txt LIKE 'GREATER CHINA' THEN 'GC'
            ELSE 'missing'
        END AS region_name,
        level_03,
        level_03_short_txt
        FROM cdl_fps_prod.silver_master_data.geo_705_flat_hier_dim
        -- K3 "(I)EUROPE EM" is end-dated but kept as its own sub-region here:
        -- remapping it to XH would duplicate the table's business key
        WHERE valid_to = '99991231' OR level_03 = 'K3'
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
    LEFT JOIN app_fps_prod.inbound_supply_chain_shares.netsavings_prod_bronze_projectclassification pc
        ON ns.ProjectClassification = pc.id
    LEFT JOIN app_fps_prod.inbound_supply_chain_shares.netsavings_prod_bronze_procurementhandling ph
        ON ns.StandardProject = ph.id
    LEFT JOIN app_fps_prod.inbound_supply_chain_shares.netsavings_prod_gold_faentrytype fet
        ON ns.FAEntryTypeID = fet.FAEntryTypeID
    LEFT JOIN app_fps_prod.inbound_supply_chain_shares.netsavings_prod_gold_gcaschange gc
        ON ns.GCASChange = gc.GCASChangeID
    LEFT JOIN (
        SELECT DISTINCT tnumber, common_name
        FROM people_dim
    ) pd ON ns.TNumber = pd.tnumber
)
SELECT
    CASE
        WHEN region_name is NULL THEN 'missing'
        ELSE region_name
    END AS region_name,
    level_03_short_txt                                           AS subregion_name,
    subsector_short_name                                         AS business_unit,
    spend_pool_low_name,
    spend_pool_med_name,
    spend_pool_high_name,
    CASE
        WHEN projectclassificationdescription LIKE '%MLC%'
            THEN 'MLC Project Negotiation (F&A)'
        WHEN LOWER(ProjectName) RLIKE
             '(export|freight|transport|truck|storage|tank|import|duty|logistics|pallet|loading|mlc|container|railcar|barge|frt|ibl|isotainer|unloading|isotank)'
            THEN 'MLC Project Negotiation (ATLAS)'
        WHEN LOWER(ProjectName) RLIKE '(incoterm|localization)'
            THEN 'MLC Project Negotiation (Manual)'
        ELSE NULL
    END                                                          AS mlc_project_classification,
    projectclassificationdescription                             AS project_classification,
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
ibl_atlas_fact = spark.sql(sql)

# COMMAND ----------

ibl_atlas_fact = ibl_atlas_fact.filter(
    (
        (ibl_atlas_fact.fiscal_year_short_text >= '24/25') |
        (ibl_atlas_fact.fiscal_year_short_text.isNull())
    ) &
    (ibl_atlas_fact.mlc_project_classification.isNotNull()) &
    (ibl_atlas_fact.mlc_project_classification != '')
)

# COMMAND ----------

# DBTITLE 1,Data Transformation - SQL
sql = """
SELECT
    CASE
        WHEN r.region is NULL THEN 'missing'
        ELSE r.region
    END AS region,
    r.level_03_short_txt                                             AS subregion,
    s.subsector_short_name                                           AS business_unit,
    spl.spend_pool_low_name,
    GUVVendor                                                        AS pg_guv_id,
    Plant                                                            AS site_code,
    ProjectID                                                        AS project_id,
    ProjectName                                                      AS project_name,
    SavingsLineName                                                  AS savings_line_name,
    CAST(savingscalculationvalue AS decimal(18,3))                   AS calc_savings,
    CAST(totalsavings            AS decimal(18,3))                   AS total_savings,
    CAST(Probability             AS decimal(5,2))                    AS probability_pct,
    HelpHurt                                                         AS help_hurt_ind,
    FiscalYear                                                       AS fiscal_year_short_text,
    TotalNumberofMonths                                              AS total_mth_benefit_cnt,
    MonthsinFiscalYear                                               AS fiscal_year_mth_benefit_cnt,
    date_format(E2E_StartDate, 'yyyyMMdd')                           AS start_date,
    date_format(E2E_EndDate,   'yyyyMMdd')                           AS end_date,
    UpdatedForecastCycle                                             AS ff_cycle_mth,
    CAST(
        (TotalNumberofMonths - MonthsinFiscalYear)
        * (totalsavings * 1000)
        / TotalNumberofMonths
        * (Probability / 100)
        AS decimal(18,2)
    )                                                                AS tapa_ff_mth_prob_adj_m_usd,
    date_format(CreationDate, 'yyyyMMdd')                            AS created_date,
    date_format(UpdatedDate,  'yyyyMMdd')                            AS updated_date,
    fdd_key
FROM app_fps_prod.inbound_supply_chain_shares.netsavings_prod_gold_fdd_b4 ns
LEFT JOIN spl_hier_dap spl
    ON ns.SpendPool_Low = spl.spend_pool_low_code
LEFT JOIN (
    SELECT DISTINCT subsector_legacy_id, subsector_short_name
    FROM cdl_fps_prod.silver_master_data.mgmt_product_dim
) s
    ON ns.SubSector = s.subsector_legacy_id
LEFT JOIN (
    SELECT DISTINCT
        CASE
            WHEN level_02_short_txt LIKE '%APAC%' THEN 'APAC'
            WHEN level_02_short_txt LIKE '%EUROPE%' THEN 'EUROPE'
            WHEN level_02_short_txt LIKE '%EECAR%' THEN 'EASTERN EUROPE'
            WHEN level_02_short_txt LIKE 'NORTH AMERICA' THEN 'NA'
            WHEN level_02_short_txt LIKE 'LATIN AMERICA' THEN 'LA'
            WHEN level_02_short_txt LIKE 'IMETA' THEN 'IMETA'
            WHEN level_02_short_txt LIKE 'GREATER CHINA' THEN 'GC'
            ELSE 'missing'
        END AS region,
        level_03,
        level_03_short_txt
    FROM cdl_fps_prod.silver_master_data.geo_705_flat_hier_dim
    WHERE valid_to = '99991231'
) r
    -- K3 "(I)EUROPE EM" was end-dated in geo_705 on 2026-06-30 and replaced by XH "EUROPE AREA"
    ON CASE WHEN ns.SubRegion = 'K3' THEN 'XH' ELSE ns.SubRegion END = r.level_03;
"""
hercules_subsector_savings_fact = spark.sql(sql)

# COMMAND ----------

# DBTITLE 1,Backup saving
saveTabletemp('hercules_subsector_savings_fact', 'parquet', 'overwrite', hercules_subsector_savings_fact)
saveTabletemp('ibl_atlas_fact', 'parquet', 'overwrite', ibl_atlas_fact)

# COMMAND ----------

# DBTITLE 1,DQ check and save to SA
df_hercules_subsector_savings_fact_temp = spark.read.parquet("/mnt/mda-pipeline-temp/hercules_subsector_savings_fact")
df_ibl_atlas_fact_temp = spark.read.parquet("/mnt/mda-pipeline-temp/ibl_atlas_fact")

primary_key = ['fdd_key']
expect_table_records_to_be_unique(df_hercules_subsector_savings_fact_temp, 'hercules_subsector_savings_fact', False, primary_key, False)
if result == 'Success':
  saveTable('hercules_subsector_savings_fact', 'parquet', 'overwrite', df_hercules_subsector_savings_fact_temp)

primary_key = ['project_id', 'forecast_cycle', 'subregion_name', 'business_unit', 'category', 'start_date', 'end_date', 'fiscal_year_short_text', 'ppv_flag', 'one_time_savings']
expect_table_records_to_be_unique(df_ibl_atlas_fact_temp, 'ibl_atlas_fact', False, primary_key, False)
if result == 'Success':
    saveTable('ibl_atlas_fact', 'parquet', 'overwrite', df_ibl_atlas_fact_temp)

# COMMAND ----------

# DBTITLE 1,Data Lineage
# hercules_subsector_savings = [["/mnt/mda-pipeline-raw/fps/spend_pool_hierarchy_dap.csv",
#                 "app_fps_prod.inbound_supply_chain_shares.netsavings_prod_gold_fdd_b4",
#                 "cdl_fps_prod.silver_master_data.mgmt_product_dim",
#                 "cdl_fps_prod.silver_master_data.geo_705_flat_hier_dim"
#                 ], ["/mnt/mda-pipeline-refined/hercules_subsector_savings_fact/"]]
# source_target_logging(hercules_subsector_savings)

# ibl_atlas_fact = [[ "/mnt/mda-pipeline-raw/fps/spend_pool_hierarchy_dap.csv",
#                     "/mnt/mda-pipeline-refined/people_dim",
#                     "app_fps_prod.inbound_supply_chain_shares.netsavings_prod_gold_fdd_b4",
#                     "cdl_fps_prod.silver_master_data.geo_705_flat_hier_dim",
#                     "cdl_fps_prod.silver_master_data.mgmt_product_dim",
#                     "app_fps_prod.inbound_supply_chain_shares.netsavings_prod_bronze_projectclassification",
#                     "app_fps_prod.inbound_supply_chain_shares.netsavings_prod_bronze_procurementhandling",
#                     "app_fps_prod.inbound_supply_chain_shares.netsavings_prod_gold_faentrytype",
#                     "app_fps_prod.inbound_supply_chain_shares.netsavings_prod_gold_gcaschange"
#                 ], ["/mnt/mda-pipeline-refined/ibl_atlas_fact/"]]
# source_target_logging(ibl_atlas_fact)
