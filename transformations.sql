-- ======================================================
-- ReNew Capital Partners - ReNew_AI_Week
-- RAW → SILVER → GOLD transformations (Databricks Spark SQL)
-- Schema: main.madan_gopal
-- ======================================================

USE CATALOG main;
USE SCHEMA madan_gopal;

-- ----------------------------------------------------------------------
-- RAW COLUMN COMMENTS (light documentation)
-- ----------------------------------------------------------------------
-- ----------------------------------------------------------------------
-- RAW COLUMN COMMENTS (light documentation)
-- Note: DDL COMMENT operations can occasionally conflict with concurrent metadata updates.
-- If needed, re-run this section safely; the rest of the pipeline does not depend on it.
-- ----------------------------------------------------------------------

-- ALTER TABLE main.madan_gopal.raw_scada_telemetry_events
--   ALTER COLUMN net_mwh COMMENT 'Net energy produced in the sample interval; aggregates to daily net MWh by plant.';

-- ALTER TABLE main.madan_gopal.raw_scada_telemetry_events
--   ALTER COLUMN curtailment_window_minutes COMMENT 'Minutes of curtailment active within the settlement day (weekday-heavy post-2025-11-21 in TX solar plants).';

-- ALTER TABLE main.madan_gopal.raw_settlement_invoices_line_items
--   ALTER COLUMN baseline_revenue_usd COMMENT 'Baseline/target merchant revenue for the day and plant, used to compute lost revenue and variance.';

-- ALTER TABLE main.madan_gopal.raw_vendor_firmware_changes_tickets
--   ALTER COLUMN ticket_type COMMENT 'Vendor portal record type: firmware_release, rollout_change, incident, hotfix.';

-- ==============================
-- SILVER TABLES (CLEANED DATA)
-- ==============================

CREATE OR REPLACE TABLE main.madan_gopal.silver_eam_master AS
SELECT
  asset_id,
  plant_id,
  COALESCE(NULLIF(TRIM(plant_name), ''), 'Unknown Plant') AS plant_name,
  UPPER(COALESCE(NULLIF(TRIM(state), ''), 'UNK')) AS state,
  UPPER(COALESCE(NULLIF(TRIM(region), ''), 'UNK')) AS region,
  LOWER(COALESCE(NULLIF(TRIM(asset_type), ''), 'unknown')) AS asset_type,
  equipment_id,
  LOWER(COALESCE(NULLIF(TRIM(equipment_type), ''), 'unknown')) AS equipment_type,
  CASE
    WHEN inverter_model IS NULL THEN NULL
    WHEN TRIM(inverter_model) IN ('XG-440','XG-360','Other') THEN TRIM(inverter_model)
    ELSE 'Other'
  END AS inverter_model,
  COALESCE(NULLIF(TRIM(vendor), ''), 'Other') AS vendor,
  LOWER(COALESCE(NULLIF(TRIM(contract_type), ''), 'unknown')) AS contract_type,
  CAST(merchant_exposure_pct AS DOUBLE) AS merchant_exposure_pct,
  CAST(baseline_net_mwh_per_day AS DOUBLE) AS baseline_net_mwh_per_day,
  CAST(distribution_target_coverage AS DOUBLE) AS distribution_target_coverage,
  CAST(effective_start_date AS DATE) AS effective_start_date,
  CAST(NULLIF(effective_end_date, 'NaT') AS DATE) AS effective_end_date
FROM main.madan_gopal.raw_eam_asset_contract_master;

CREATE OR REPLACE TABLE main.madan_gopal.silver_scada_events AS
SELECT
  event_id,
  CAST(event_ts_utc AS TIMESTAMP) AS event_ts_utc,
  COALESCE(CAST(NULLIF(settlement_day, 'NaT') AS DATE), CAST(CAST(event_ts_utc AS TIMESTAMP) AS DATE)) AS settlement_day,
  plant_id,
  UPPER(COALESCE(NULLIF(TRIM(state), ''), 'UNK')) AS state,
  UPPER(COALESCE(NULLIF(TRIM(region), ''), 'UNK')) AS region,
  LOWER(COALESCE(NULLIF(TRIM(asset_type), ''), 'unknown')) AS asset_type,
  equipment_id,
  LOWER(COALESCE(NULLIF(TRIM(equipment_type), ''), 'unknown')) AS equipment_type,
  CASE
    WHEN inverter_model IS NULL THEN NULL
    WHEN TRIM(inverter_model) IN ('XG-440','XG-360','Other') THEN TRIM(inverter_model)
    ELSE 'Other'
  END AS inverter_model,
  NULLIF(TRIM(firmware_version), '') AS firmware_version,
  LOWER(COALESCE(NULLIF(TRIM(status), ''), 'unknown')) AS status,
  CAST(net_mwh AS DOUBLE) AS net_mwh,
  CAST(availability_flag AS BOOLEAN) AS availability_flag,
  CAST(downtime_minutes AS INT) AS downtime_minutes,
  CAST(curtailment_flag AS BOOLEAN) AS curtailment_flag,
  CAST(curtailment_window_minutes AS INT) AS curtailment_window_minutes,
  NULLIF(TRIM(alarm_code), '') AS alarm_code,
  CAST(CAST(event_ts_utc AS TIMESTAMP) AS DATE) AS event_date,
  DAYOFWEEK(CAST(CAST(event_ts_utc AS TIMESTAMP) AS DATE)) AS day_of_week
FROM main.madan_gopal.raw_scada_telemetry_events;

CREATE OR REPLACE TABLE main.madan_gopal.silver_cmms_work_orders AS
SELECT
  work_order_id,
  CAST(created_ts_utc AS TIMESTAMP) AS created_ts_utc,
  CAST(NULLIF(completed_ts_utc, 'NaT') AS TIMESTAMP) AS completed_ts_utc,
  CAST(CAST(created_ts_utc AS TIMESTAMP) AS DATE) AS created_date,
  CASE WHEN NULLIF(completed_ts_utc, 'NaT') IS NOT NULL THEN CAST(CAST(NULLIF(completed_ts_utc, 'NaT') AS TIMESTAMP) AS DATE) END AS completed_date,
  plant_id,
  equipment_id,
  LOWER(COALESCE(NULLIF(TRIM(equipment_type), ''), 'unknown')) AS equipment_type,
  LOWER(COALESCE(NULLIF(TRIM(work_order_type), ''), 'unknown')) AS work_order_type,
  LOWER(COALESCE(NULLIF(TRIM(issue_category), ''), 'other')) AS issue_category,
  COALESCE(NULLIF(TRIM(vendor), ''), 'Other') AS vendor,
  CAST(labor_hours AS DOUBLE) AS labor_hours,
  CAST(parts_cost_usd AS DOUBLE) AS parts_cost_usd,
  CAST(labor_cost_usd AS DOUBLE) AS labor_cost_usd,
  CAST(COALESCE(labor_cost_usd, 0.0) + COALESCE(parts_cost_usd, 0.0) AS DOUBLE) AS om_cost_usd,
  dispatch_id,
  technician_id,
  CAST(dispatch_ts_utc AS TIMESTAMP) AS dispatch_ts_utc,
  vendor_ticket_id,
  CASE
    WHEN NULLIF(completed_ts_utc, 'NaT') IS NOT NULL THEN ROUND((UNIX_TIMESTAMP(CAST(NULLIF(completed_ts_utc, 'NaT') AS TIMESTAMP)) - UNIX_TIMESTAMP(CAST(created_ts_utc AS TIMESTAMP))) / 3600.0, 2)
    ELSE NULL
  END AS wo_cycle_hours
FROM main.madan_gopal.raw_cmms_work_orders_dispatch;

CREATE OR REPLACE TABLE main.madan_gopal.silver_settlement_lines AS
SELECT
  invoice_id,
  invoice_line_id,
  CAST(NULLIF(settlement_day, 'NaT') AS DATE) AS settlement_day,
  plant_id,
  UPPER(COALESCE(NULLIF(TRIM(state), ''), 'UNK')) AS state,
  LOWER(COALESCE(NULLIF(TRIM(asset_type), ''), 'unknown')) AS asset_type,
  LOWER(COALESCE(NULLIF(TRIM(charge_type), ''), 'other')) AS charge_type,
  CAST(quantity_mwh AS DOUBLE) AS quantity_mwh,
  CAST(amount_usd AS DOUBLE) AS amount_usd,
  CAST(market_price_usd_per_mwh AS DOUBLE) AS market_price_usd_per_mwh,
  CAST(baseline_revenue_usd AS DOUBLE) AS baseline_revenue_usd,
  LOWER(COALESCE(NULLIF(TRIM(imbalance_charge_reason), ''), 'other')) AS imbalance_charge_reason,
  CASE WHEN LOWER(TRIM(charge_type)) = 'merchant_revenue' THEN CAST(amount_usd AS DOUBLE) ELSE 0.0 END AS merchant_revenue_usd,
  CASE WHEN LOWER(TRIM(charge_type)) = 'penalty' THEN CAST(amount_usd AS DOUBLE) ELSE 0.0 END AS penalties_usd,
  CASE WHEN LOWER(TRIM(charge_type)) = 'imbalance_charge' THEN CAST(amount_usd AS DOUBLE) ELSE 0.0 END AS imbalance_charges_usd
FROM main.madan_gopal.raw_settlement_invoices_line_items;

CREATE OR REPLACE TABLE main.madan_gopal.silver_vendor_changes AS
SELECT
  ticket_id,
  LOWER(COALESCE(NULLIF(TRIM(ticket_type), ''), 'unknown')) AS ticket_type,
  CAST(created_ts_utc AS TIMESTAMP) AS created_ts_utc,
  CAST(change_start_ts_utc AS TIMESTAMP) AS change_start_ts_utc,
  CAST(change_end_ts_utc AS TIMESTAMP) AS change_end_ts_utc,
  CAST(CAST(change_start_ts_utc AS TIMESTAMP) AS DATE) AS change_start_date,
  CAST(CAST(change_end_ts_utc AS TIMESTAMP) AS DATE) AS change_end_date,
  plant_id,
  equipment_id,
  LOWER(COALESCE(NULLIF(TRIM(equipment_type), ''), 'inverter')) AS equipment_type,
  CASE
    WHEN inverter_model IS NULL THEN NULL
    WHEN TRIM(inverter_model) IN ('XG-440','XG-360','Other') THEN TRIM(inverter_model)
    ELSE 'Other'
  END AS inverter_model,
  NULLIF(TRIM(from_firmware_version), '') AS from_firmware_version,
  NULLIF(TRIM(to_firmware_version), '') AS to_firmware_version,
  COALESCE(NULLIF(TRIM(vendor), ''), 'Other') AS vendor,
  LOWER(COALESCE(NULLIF(TRIM(change_reason), ''), 'other')) AS change_reason,
  LOWER(COALESCE(NULLIF(TRIM(status), ''), 'unknown')) AS status,
  summary
FROM main.madan_gopal.raw_vendor_firmware_changes_tickets;

-- ==============================
-- GOLD TABLES (AGGREGATED DATA)
-- ==============================

-- Daily generation at plant/day for dashboard + weekday curtailment pattern
CREATE OR REPLACE TABLE main.madan_gopal.gold_daily_generation AS
WITH plant_dim AS (
  SELECT
    plant_id,
    MAX(plant_name) AS plant_name,
    MAX(state) AS state,
    MAX(region) AS region,
    MAX(asset_type) AS asset_type,
    MAX(baseline_net_mwh_per_day) AS plant_baseline_mwh_per_day
  FROM main.madan_gopal.silver_eam_master
  GROUP BY plant_id
),
plant_day AS (
  SELECT
    s.settlement_day,
    s.plant_id,
    MAX(s.state) AS state,
    MAX(s.region) AS region,
    MAX(s.asset_type) AS asset_type,
    SUM(COALESCE(s.net_mwh, 0.0)) AS net_mwh,
    SUM(COALESCE(s.curtailment_window_minutes, 0)) AS curtailment_window_minutes,
    MAX(CASE WHEN s.curtailment_flag THEN 1 ELSE 0 END) AS curtailment_flag_any,
    DAYOFWEEK(s.settlement_day) AS day_of_week
  FROM main.madan_gopal.silver_scada_events s
  GROUP BY s.settlement_day, s.plant_id
)
SELECT
  pd.settlement_day,
  pd.plant_id,
  d.plant_name,
  COALESCE(pd.asset_type, d.asset_type) AS asset_type,
  COALESCE(pd.state, d.state) AS state,
  COALESCE(pd.region, d.region) AS region,
  pd.day_of_week,
  pd.net_mwh,
  -- trailing 30-day baseline (plant-level)
  ROUND(
    AVG(pd.net_mwh) OVER (
      PARTITION BY pd.plant_id
      ORDER BY pd.settlement_day
      ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ),
    3
  ) AS baseline_mwh,
  ROUND(pd.net_mwh - AVG(pd.net_mwh) OVER (
      PARTITION BY pd.plant_id
      ORDER BY pd.settlement_day
      ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ), 3) AS variance_mwh,
  CAST(pd.curtailment_window_minutes AS INT) AS curtailment_window_minutes,
  CAST(pd.curtailment_flag_any AS BOOLEAN) AS curtailment_flag_any,
  CAST(d.plant_baseline_mwh_per_day AS DOUBLE) AS baseline_mwh_per_day_target
FROM plant_day pd
LEFT JOIN plant_dim d
  ON pd.plant_id = d.plant_id;

-- Availability and downtime by equipment type/model/firmware
CREATE OR REPLACE TABLE main.madan_gopal.gold_asset_availability AS
WITH base AS (
  SELECT
    s.settlement_day,
    s.plant_id,
    MAX(s.state) AS state,
    MAX(s.region) AS region,
    MAX(s.asset_type) AS asset_type,
    LOWER(COALESCE(NULLIF(TRIM(s.equipment_type), ''), 'unknown')) AS equipment_type,
    CASE
      WHEN s.equipment_type = 'inverter' THEN COALESCE(s.inverter_model, 'Other')
      ELSE 'Other'
    END AS inverter_model,
    COALESCE(s.firmware_version, 'unknown') AS firmware_version,
    AVG(CASE WHEN s.availability_flag THEN 1.0 ELSE 0.0 END) AS availability_pct,
    SUM(COALESCE(s.downtime_minutes, 0)) AS downtime_minutes,
    COUNT_IF(s.status = 'tripped') AS trip_event_count,
    COUNT_IF(s.status = 'derated') AS derate_event_count
  FROM main.madan_gopal.silver_scada_events s
  GROUP BY
    s.settlement_day,
    s.plant_id,
    LOWER(COALESCE(NULLIF(TRIM(s.equipment_type), ''), 'unknown')),
    CASE WHEN s.equipment_type = 'inverter' THEN COALESCE(s.inverter_model, 'Other') ELSE 'Other' END,
    COALESCE(s.firmware_version, 'unknown')
)
SELECT
  b.settlement_day,
  b.plant_id,
  d.plant_name,
  b.asset_type,
  b.state,
  b.region,
  b.equipment_type,
  CASE
    WHEN b.equipment_type = 'inverter' AND b.inverter_model IN ('XG-440','XG-360') THEN b.inverter_model
    WHEN b.equipment_type = 'inverter' THEN 'Other'
    ELSE 'Other'
  END AS inverter_model,
  b.firmware_version,
  ROUND(b.availability_pct, 4) AS availability_pct,
  CAST(b.downtime_minutes AS BIGINT) AS downtime_minutes,
  CAST(b.trip_event_count AS BIGINT) AS trip_event_count,
  CAST(b.derate_event_count AS BIGINT) AS derate_event_count
FROM base b
LEFT JOIN (
  SELECT plant_id, MAX(plant_name) AS plant_name
  FROM main.madan_gopal.silver_eam_master
  GROUP BY plant_id
) d
ON b.plant_id = d.plant_id;

-- O&M spend (with net MWh join for cost/MWh)
CREATE OR REPLACE TABLE main.madan_gopal.gold_om_costs AS
WITH wo_day AS (
  SELECT
    w.created_date AS date,
    w.plant_id,
    MAX(e.asset_type) AS asset_type,
    MAX(e.state) AS state,
    MAX(e.region) AS region,
    w.equipment_type,
    w.work_order_type,
    w.vendor,
    SUM(COALESCE(w.om_cost_usd, 0.0)) AS om_cost_usd,
    SUM(COALESCE(w.labor_hours, 0.0)) AS labor_hours,
    COUNT_IF(w.work_order_type = 'corrective') AS corrective_wo_count,
    COUNT(*) AS total_wo_count
  FROM main.madan_gopal.silver_cmms_work_orders w
  LEFT JOIN (
    SELECT plant_id, equipment_id, MAX(asset_type) AS asset_type, MAX(state) AS state, MAX(region) AS region
    FROM main.madan_gopal.silver_eam_master
    GROUP BY plant_id, equipment_id
  ) e
    ON w.plant_id = e.plant_id AND w.equipment_id = e.equipment_id
  GROUP BY
    w.created_date,
    w.plant_id,
    w.equipment_type,
    w.work_order_type,
    w.vendor
)
SELECT
  wo.date,
  wo.plant_id,
  p.plant_name,
  wo.asset_type,
  wo.state,
  wo.region,
  wo.equipment_type,
  wo.work_order_type,
  wo.vendor,
  ROUND(wo.om_cost_usd, 2) AS om_cost_usd,
  ROUND(wo.labor_hours, 2) AS labor_hours,
  CAST(wo.corrective_wo_count AS BIGINT) AS corrective_wo_count,
  CAST(wo.total_wo_count AS BIGINT) AS total_wo_count,
  ROUND(gd.net_mwh, 3) AS net_mwh,
  ROUND(wo.om_cost_usd / NULLIF(gd.net_mwh, 0.0), 4) AS cost_per_mwh_usd
FROM wo_day wo
LEFT JOIN main.madan_gopal.gold_daily_generation gd
  ON wo.date = gd.settlement_day AND wo.plant_id = gd.plant_id
LEFT JOIN (
  SELECT plant_id, MAX(plant_name) AS plant_name
  FROM main.madan_gopal.silver_eam_master
  GROUP BY plant_id
) p
  ON wo.plant_id = p.plant_id;

-- Settlement variance and cumulative business impact
CREATE OR REPLACE TABLE main.madan_gopal.gold_revenue_variance AS
WITH daily AS (
  SELECT
    s.settlement_day,
    s.plant_id,
    MAX(e.plant_name) AS plant_name,
    MAX(COALESCE(e.asset_type, s.asset_type)) AS asset_type,
    MAX(COALESCE(e.state, s.state)) AS state,
    MAX(e.region) AS region,
    SUM(s.merchant_revenue_usd) AS merchant_revenue_usd,
    SUM(s.penalties_usd) AS penalties_usd,
    SUM(s.imbalance_charges_usd) AS imbalance_charges_usd,
    SUM(COALESCE(s.baseline_revenue_usd, 0.0)) AS baseline_revenue_usd
  FROM main.madan_gopal.silver_settlement_lines s
  LEFT JOIN (
    SELECT plant_id, MAX(plant_name) AS plant_name, MAX(asset_type) AS asset_type, MAX(state) AS state, MAX(region) AS region
    FROM main.madan_gopal.silver_eam_master
    GROUP BY plant_id
  ) e
    ON s.plant_id = e.plant_id
  GROUP BY s.settlement_day, s.plant_id
)
SELECT
  d.settlement_day,
  d.plant_id,
  d.plant_name,
  d.asset_type,
  d.state,
  d.region,
  ROUND(d.merchant_revenue_usd, 2) AS merchant_revenue_usd,
  ROUND(d.penalties_usd, 2) AS penalties_usd,
  ROUND(d.imbalance_charges_usd, 2) AS imbalance_charges_usd,
  ROUND(d.baseline_revenue_usd, 2) AS baseline_revenue_usd,
  ROUND(GREATEST(d.baseline_revenue_usd - d.merchant_revenue_usd, 0.0), 2) AS lost_revenue_usd,
  ROUND((d.merchant_revenue_usd + d.penalties_usd + d.imbalance_charges_usd) - d.baseline_revenue_usd, 2) AS total_variance_usd,
  ROUND(
    SUM(
      GREATEST(d.baseline_revenue_usd - d.merchant_revenue_usd, 0.0)
      + ABS(d.penalties_usd)
      + ABS(d.imbalance_charges_usd)
    ) OVER (PARTITION BY d.plant_id ORDER BY d.settlement_day),
    2
  ) AS cumulative_impact_usd
FROM daily d;

-- Root-cause alignment table for drill-down (affected plants, tickets, top causes)
CREATE OR REPLACE TABLE main.madan_gopal.gold_firmware_impact AS
WITH plant_dim AS (
  SELECT
    plant_id,
    MAX(plant_name) AS plant_name,
    MAX(state) AS state,
    MAX(region) AS region,
    MAX(asset_type) AS asset_type
  FROM main.madan_gopal.silver_eam_master
  GROUP BY plant_id
),
days AS (
  SELECT DISTINCT settlement_day, plant_id
  FROM main.madan_gopal.gold_daily_generation
),
-- vendor tickets active on the day (change window spans multiple days)
tickets_by_day AS (
  SELECT
    d.settlement_day,
    v.plant_id,
    COALESCE(v.inverter_model, 'Other') AS inverter_model,
    COUNT(*) AS vendor_ticket_count,
    MAX(CASE WHEN v.ticket_type = 'rollout_change' THEN 1 ELSE 0 END) AS has_rollout_ticket,
    MAX(CASE WHEN v.ticket_type = 'hotfix' THEN 1 ELSE 0 END) AS has_hotfix_ticket
  FROM days d
  JOIN main.madan_gopal.silver_vendor_changes v
    ON d.plant_id = v.plant_id
   AND d.settlement_day BETWEEN v.change_start_date AND v.change_end_date
  GROUP BY d.settlement_day, v.plant_id, COALESCE(v.inverter_model, 'Other')
),
-- daily firmware mode from scada for inverters
firmware_mode AS (
  SELECT
    settlement_day,
    plant_id,
    inverter_model,
    MAX_BY(firmware_version, fw_cnt) AS firmware_version_mode
  FROM (
    SELECT
      settlement_day,
      plant_id,
      COALESCE(inverter_model, 'Other') AS inverter_model,
      COALESCE(firmware_version, 'unknown') AS firmware_version,
      COUNT(*) AS fw_cnt
    FROM main.madan_gopal.silver_scada_events
    WHERE equipment_type = 'inverter'
    GROUP BY settlement_day, plant_id, COALESCE(inverter_model, 'Other'), COALESCE(firmware_version, 'unknown')
  ) x
  GROUP BY settlement_day, plant_id, inverter_model
),
-- top alarm code by plant/day (inverter-focused)
alarm_ranked AS (
  SELECT
    settlement_day,
    plant_id,
    COALESCE(alarm_code, 'OTHER-00') AS alarm_code,
    COUNT(*) AS c,
    ROW_NUMBER() OVER (PARTITION BY settlement_day, plant_id ORDER BY COUNT(*) DESC) AS rn
  FROM main.madan_gopal.silver_scada_events
  WHERE equipment_type = 'inverter'
  GROUP BY settlement_day, plant_id, COALESCE(alarm_code, 'OTHER-00')
),
top_alarm AS (
  SELECT settlement_day, plant_id, alarm_code AS top_alarm_code
  FROM alarm_ranked
  WHERE rn = 1
),
-- top downtime cause (CMMS issue_category) by plant/day
cause_ranked AS (
  SELECT
    created_date AS settlement_day,
    plant_id,
    COALESCE(issue_category, 'other') AS issue_category,
    COUNT(*) AS c,
    ROW_NUMBER() OVER (PARTITION BY created_date, plant_id ORDER BY COUNT(*) DESC) AS rn
  FROM main.madan_gopal.silver_cmms_work_orders
  GROUP BY created_date, plant_id, COALESCE(issue_category, 'other')
),
top_cause AS (
  SELECT settlement_day, plant_id, issue_category AS top_downtime_cause
  FROM cause_ranked
  WHERE rn = 1
),
corrective_counts AS (
  SELECT
    created_date AS settlement_day,
    plant_id,
    COUNT_IF(work_order_type = 'corrective') AS corrective_wo_count
  FROM main.madan_gopal.silver_cmms_work_orders
  GROUP BY created_date, plant_id
),
inv_avail AS (
  SELECT
    settlement_day,
    plant_id,
    inverter_model,
    AVG(availability_pct) AS availability_pct,
    SUM(downtime_minutes) AS downtime_minutes
  FROM main.madan_gopal.gold_asset_availability
  WHERE equipment_type = 'inverter'
  GROUP BY settlement_day, plant_id, inverter_model
),
rev AS (
  SELECT
    settlement_day,
    plant_id,
    SUM(lost_revenue_usd) AS lost_revenue_usd,
    SUM(ABS(penalties_usd)) AS penalties_abs_usd,
    SUM(ABS(imbalance_charges_usd)) AS imbalance_abs_usd
  FROM main.madan_gopal.gold_revenue_variance
  GROUP BY settlement_day, plant_id
)
SELECT
  d.plant_id,
  p.plant_name,
  p.state,
  p.region,
  p.asset_type,
  d.settlement_day,
  COALESCE(t.inverter_model, f.inverter_model, 'Other') AS inverter_model,
  COALESCE(f.firmware_version_mode, 'unknown') AS firmware_version_mode,
  COALESCE(t.vendor_ticket_count, 0) AS vendor_ticket_count,
  CASE WHEN d.settlement_day BETWEEN DATE('2025-11-20') AND DATE('2025-11-21') THEN TRUE ELSE FALSE END AS rollout_flag,
  CASE WHEN d.settlement_day = DATE('2025-12-08') THEN TRUE ELSE FALSE END AS hotfix_flag,
  ROUND(gd.variance_mwh, 3) AS net_mwh_variance_total,
  ROUND(COALESCE(a.availability_pct, 0.0), 4) AS availability_pct,
  CAST(COALESCE(a.downtime_minutes, 0) AS BIGINT) AS downtime_minutes,
  COALESCE(ta.top_alarm_code, 'OTHER-00') AS top_alarm_code,
  COALESCE(tc.top_downtime_cause, 'other') AS top_downtime_cause,
  CAST(COALESCE(cc.corrective_wo_count, 0) AS BIGINT) AS corrective_wo_count,
  ROUND(COALESCE(r.lost_revenue_usd, 0.0), 2) AS lost_revenue_usd,
  ROUND(COALESCE(r.penalties_abs_usd, 0.0), 2) AS penalties_usd,
  ROUND(COALESCE(r.imbalance_abs_usd, 0.0), 2) AS imbalance_charges_usd
FROM days d
LEFT JOIN plant_dim p ON d.plant_id = p.plant_id
LEFT JOIN main.madan_gopal.gold_daily_generation gd
  ON d.settlement_day = gd.settlement_day AND d.plant_id = gd.plant_id
LEFT JOIN tickets_by_day t
  ON d.settlement_day = t.settlement_day AND d.plant_id = t.plant_id
LEFT JOIN firmware_mode f
  ON d.settlement_day = f.settlement_day AND d.plant_id = f.plant_id
LEFT JOIN inv_avail a
  ON d.settlement_day = a.settlement_day AND d.plant_id = a.plant_id
 AND a.inverter_model = COALESCE(t.inverter_model, f.inverter_model, a.inverter_model)
LEFT JOIN top_alarm ta
  ON d.settlement_day = ta.settlement_day AND d.plant_id = ta.plant_id
LEFT JOIN top_cause tc
  ON d.settlement_day = tc.settlement_day AND d.plant_id = tc.plant_id
LEFT JOIN corrective_counts cc
  ON d.settlement_day = cc.settlement_day AND d.plant_id = cc.plant_id
LEFT JOIN rev r
  ON d.settlement_day = r.settlement_day AND d.plant_id = r.plant_id;

-- ==============================
-- TABLE DESCRIPTIONS
-- ==============================

ALTER TABLE main.madan_gopal.silver_eam_master
  SET TBLPROPERTIES ('comment' = 'Clean EAM dimensional backbone for plants/equipment and commercial context (contract_type, merchant_exposure_pct, baseline_net_mwh_per_day). Used to enrich SCADA, CMMS, and settlements with consistent keys and dashboard filters.');

ALTER TABLE main.madan_gopal.silver_scada_events
  SET TBLPROPERTIES ('comment' = 'Cleaned atomic SCADA telemetry/events with standardized enums and derived event_date/day_of_week plus a reliable settlement_day. Primary source for generation, availability, downtime, curtailment, alarm_code, and firmware_version signals.');

ALTER TABLE main.madan_gopal.silver_cmms_work_orders
  SET TBLPROPERTIES ('comment' = 'Cleaned CMMS work orders/dispatch data with derived created_date/completed_date, wo_cycle_hours, and om_cost_usd. Supports O&M spend, corrective dispatch intensity, and correlation to vendor tickets.');

ALTER TABLE main.madan_gopal.silver_settlement_lines
  SET TBLPROPERTIES ('comment' = 'Cleaned settlement invoice line items with standardized charge types and derived revenue components (merchant_revenue_usd, penalties_usd, imbalance_charges_usd) plus baseline_revenue_usd for variance reporting.');

ALTER TABLE main.madan_gopal.silver_vendor_changes
  SET TBLPROPERTIES ('comment' = 'Cleaned vendor portal change/ticket records for inverter firmware releases, rollout changes (2025-11-20..21), incidents, and hotfix (2025-12-08). Provides dated root-cause context by plant and model.');

ALTER TABLE main.madan_gopal.gold_daily_generation
  SET TBLPROPERTIES ('comment' = 'Daily plant-level net generation and trailing 30-day baseline used to reveal the 2025-11-21 step-change (portfolio 30d avg ~18,500 → ~15,900 MWh/day) and weekday-heavy curtailment pattern. Includes variance_mwh, day_of_week, and curtailment minutes.');

ALTER TABLE main.madan_gopal.gold_asset_availability
  SET TBLPROPERTIES ('comment' = 'Daily availability/downtime by plant, equipment_type, inverter_model bucket, and firmware_version. Highlights XG-440 inverter availability decline post-2025-11-21 while wind turbines remain stable.');

ALTER TABLE main.madan_gopal.gold_om_costs
  SET TBLPROPERTIES ('comment' = 'Daily O&M spend and dispatch intensity by plant/equipment/work_order_type/vendor with cost_per_mwh via join to gold_daily_generation. Designed to show ~$260K incremental O&M through 2025-12-15 driven by corrective inverter work.');

ALTER TABLE main.madan_gopal.gold_revenue_variance
  SET TBLPROPERTIES ('comment' = 'Daily settlement financials vs baseline by plant with lost_revenue_usd, penalties, imbalance charges, and cumulative_impact_usd. Designed to accumulate to ~$1.42M impact through 2025-12-15 starting 2025-11-21.');

ALTER TABLE main.madan_gopal.gold_firmware_impact
  SET TBLPROPERTIES ('comment' = 'Curated drill-down table aligning firmware rollout/hotfix tickets to operational degradation (availability/downtime, alarm codes, corrective WOs) and financial impact (lost revenue, penalties, imbalance) by plant and day. Powers the affected-plants evidence table in the executive dashboard.');
