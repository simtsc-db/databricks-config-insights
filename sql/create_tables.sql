-- Configuration Insights - Schema Setup (REFERENCE ONLY)
-- Tables and views are created automatically by the collector job; this file
-- documents the schema. All objects live in ONE configurable location:
--   <catalog>.<schema>   (DAB variables `catalog` + `schema`; defaults main.config_insights)
-- Replace `main.config_insights` below with your own catalog.schema if different.
-- Tables use schema evolution: new fields from the API are added automatically.
-- Drift detection is handled by Lakehouse Monitoring (no custom drift table needed).

-- The catalog must already exist; the job creates the schema.
CREATE SCHEMA IF NOT EXISTS main.config_insights;

-- Primary settings history table (append-only, TimeSeries layout)
-- Each collection run appends a full snapshot of all discovered settings.
-- Schema evolution (mergeSchema=true) adds new columns automatically if
-- the Settings V2 API returns new metadata fields in future.
CREATE TABLE IF NOT EXISTS main.config_insights.settings_history (
    collected_at TIMESTAMP NOT NULL
        COMMENT 'Timestamp of this collection run (TimeSeries key for Lakehouse Monitoring)',
    account_id STRING NOT NULL
        COMMENT 'Databricks account ID',
    scope STRING NOT NULL
        COMMENT 'Setting scope: account or workspace',
    workspace_id BIGINT
        COMMENT 'Workspace ID (NULL for account-level settings)',
    workspace_name STRING
        COMMENT 'Workspace display name (NULL for account-level settings)',
    setting_name STRING NOT NULL
        COMMENT 'Setting key as returned by the Settings V2 metadata API',
    setting_value STRING
        COMMENT 'Current value of the setting (string representation)',
    setting_type STRING
        COMMENT 'Type field from Settings V2 metadata (e.g., BooleanSetting, StringSetting)',
    source STRING
        COMMENT 'Collection source: settings_v2',
    category STRING
        COMMENT 'Inferred category: security, compute, data, network, identity, preview, general',
    preview_phase STRING
        COMMENT 'Preview phase from metadata: PRIVATE_PREVIEW, BETA, PUBLIC_PREVIEW, GA, or NULL',
    description STRING
        COMMENT 'Human-readable description from Settings V2 metadata'
)
USING DELTA
COMMENT 'Append-only history of all Databricks settings collected via dynamic discovery. Schema evolves automatically.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.columnMapping.mode' = 'name'
);

-- View: Latest snapshot (most recent collection run only)
CREATE OR REPLACE VIEW main.config_insights.settings_latest AS
WITH latest AS (
    SELECT MAX(collected_at) AS max_ts
    FROM main.config_insights.settings_history
)
SELECT s.*
FROM main.config_insights.settings_history s
INNER JOIN latest l ON s.collected_at = l.max_ts;

-- View: Workspace comparison (identifies inconsistencies)
CREATE OR REPLACE VIEW main.config_insights.workspace_comparison AS
WITH latest AS (
    SELECT MAX(collected_at) AS max_ts
    FROM main.config_insights.settings_history
),
current_settings AS (
    SELECT s.*
    FROM main.config_insights.settings_history s
    INNER JOIN latest l ON s.collected_at = l.max_ts
    WHERE s.scope = 'workspace'
),
agg AS (
    SELECT
        setting_name,
        category,
        COUNT(DISTINCT setting_value) AS distinct_values,
        COUNT(DISTINCT workspace_id) AS workspace_count,
        FIRST(setting_value) AS sample_value
    FROM current_settings
    GROUP BY setting_name, category
)
SELECT
    a.setting_name,
    a.category,
    a.distinct_values,
    a.workspace_count,
    CASE
        WHEN a.distinct_values > 1 THEN 'INCONSISTENT'
        ELSE 'CONSISTENT'
    END AS consistency_status,
    a.sample_value
FROM agg a;

-- View: Preview features status across workspaces
CREATE OR REPLACE VIEW main.config_insights.preview_features AS
WITH latest AS (
    SELECT MAX(collected_at) AS max_ts
    FROM main.config_insights.settings_history
)
SELECT
    s.workspace_name,
    s.workspace_id,
    s.setting_name,
    s.description,
    s.setting_value,
    s.preview_phase,
    CASE
        WHEN LOWER(s.setting_value) IN ('true', 'enabled', '1', 'on') THEN 'ENABLED'
        WHEN LOWER(s.setting_value) IN ('false', 'disabled', '0', 'off') THEN 'DISABLED'
        ELSE s.setting_value
    END AS status
FROM main.config_insights.settings_history s
INNER JOIN latest l ON s.collected_at = l.max_ts
WHERE s.preview_phase IS NOT NULL
  AND s.preview_phase NOT IN ('GA', 'None', '', 'PreviewPhase.GA')
ORDER BY s.setting_name, s.workspace_name;

-- View: Preview features heatmap (for dashboard visualization)
-- Produces one row per (feature, workspace) with normalized status
CREATE OR REPLACE VIEW main.config_insights.preview_heatmap AS
WITH latest AS (
    SELECT MAX(collected_at) AS max_ts
    FROM main.config_insights.settings_history
)
SELECT
    s.setting_name AS feature,
    s.description,
    s.preview_phase,
    s.workspace_name,
    s.workspace_id,
    s.setting_value,
    CASE
        WHEN LOWER(s.setting_value) IN ('true', 'enabled', '1', 'on') THEN 'ENABLED'
        WHEN LOWER(s.setting_value) IN ('false', 'disabled', '0', 'off') THEN 'DISABLED'
        ELSE 'OTHER'
    END AS status
FROM main.config_insights.settings_history s
INNER JOIN latest l ON s.collected_at = l.max_ts
WHERE s.preview_phase IS NOT NULL
  AND s.preview_phase NOT IN ('GA', 'None', '', 'PreviewPhase.GA')
  AND s.scope = 'workspace'
ORDER BY s.setting_name, s.workspace_name;

-- Note: Drift detection is handled by Lakehouse Monitoring (TimeSeries profile).
-- After the monitor is created, drift metrics are automatically written to:
--   main.config_insights.settings_history_drift_metrics
-- Profile metrics are written to:
--   main.config_insights.settings_history_profile_metrics
-- These tables are auto-generated and maintained by the platform.
--
-- WHY TimeSeries (not Snapshot):
-- - Our table is append-only with collected_at as the time axis
-- - TimeSeries computes CONSECUTIVE drift (today vs yesterday)
-- - TimeSeries supports incremental processing via CDF
-- - Snapshot reprocesses the full table each refresh and only supports baseline drift
