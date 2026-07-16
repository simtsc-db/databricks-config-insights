-- Configuration Insights - Schema Setup (REFERENCE ONLY)
-- Tables and views are created automatically by the collector job; this file
-- documents the schema. All objects live in ONE configurable location:
--   <catalog>.<schema>   (DAB variables `catalog` + `schema`; defaults main.config_insights)
-- Replace `main.config_insights` below with your own catalog.schema if different.
-- Tables use schema evolution: new fields from the API are added automatically.
--
-- Data model (single source of truth per concern):
--   settings_history      append-only snapshot log (one row per setting per run)
--   setting_category_map  setting_name -> functional category (from ai_classify)
--   settings_latest       enriched current state (category from the map, is_preview, status)
--   settings_drift        value_changed / added / removed vs the previous snapshot
--   workspace_comparison  cross-workspace consistency
-- Category is resolved from setting_category_map in every view, so it never
-- goes stale on old snapshots.

-- The catalog must already exist; the job creates the schema.
CREATE SCHEMA IF NOT EXISTS main.config_insights;

-- Primary settings history table (append-only snapshot log)
-- Each collection run appends a full snapshot of all discovered settings.
-- Schema evolution (mergeSchema=true) adds new columns automatically if
-- the Settings V2 API returns new metadata fields in future.
CREATE TABLE IF NOT EXISTS main.config_insights.settings_history (
    collected_at TIMESTAMP NOT NULL
        COMMENT 'Timestamp of this collection run (snapshot key for drift comparison)',
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
        COMMENT 'Current value of the setting (string representation). Sentinels: <unavailable> / <null> / <not-set> mean the value could not be read',
    setting_type STRING
        COMMENT 'Type field from Settings V2 metadata (e.g., BooleanSetting, StringSetting)',
    source STRING
        COMMENT 'Collection source: settings_v2',
    category STRING
        COMMENT 'Category recorded at collection time; for display, use the category from setting_category_map instead (the views do this)',
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

-- Category map: single source of truth for a setting's functional category.
-- Populated by the collector via ai_classify (configurable label set). Keyed by
-- setting_name + a hash of the active label set, so only new/changed settings
-- are re-classified.
CREATE TABLE IF NOT EXISTS main.config_insights.setting_category_map (
    setting_name STRING
        COMMENT 'Setting key',
    category STRING
        COMMENT 'Functional category chosen by ai_classify (e.g., governance, ingestion, AI, ML, compute, ...)',
    labels_version STRING
        COMMENT 'Hash of the active category label set; a change triggers reclassification',
    classified_at TIMESTAMP
        COMMENT 'When this setting was last classified'
)
USING DELTA
COMMENT 'setting_name -> functional category. Single source of truth for category.';

-- View: Enriched current state (powers the whole Overview page)
-- Latest snapshot + category from the map + derived is_preview / status.
CREATE OR REPLACE VIEW main.config_insights.settings_latest AS
WITH latest AS (
    SELECT MAX(collected_at) AS max_ts FROM main.config_insights.settings_history
),
cur_cat AS (
    SELECT setting_name, category FROM (
        SELECT setting_name, category,
               ROW_NUMBER() OVER (PARTITION BY setting_name ORDER BY classified_at DESC) AS rn
        FROM main.config_insights.setting_category_map
    ) WHERE rn = 1
)
SELECT
    s.collected_at, s.account_id, s.scope, s.workspace_id, s.workspace_name,
    s.setting_name, s.setting_value, s.setting_type, s.source,
    COALESCE(cc.category, s.category) AS category,
    s.preview_phase, s.description,
    (s.preview_phase IS NOT NULL
        AND s.preview_phase NOT IN ('GA', 'None', '', 'PreviewPhase.GA')) AS is_preview,
    CASE
        WHEN LOWER(s.setting_value) IN ('true', 'enabled', '1', 'on') THEN 'ENABLED'
        WHEN LOWER(s.setting_value) IN ('false', 'disabled', '0', 'off') THEN 'DISABLED'
        ELSE 'OTHER'
    END AS status
FROM main.config_insights.settings_history s
INNER JOIN latest l ON s.collected_at = l.max_ts
LEFT JOIN cur_cat cc ON cc.setting_name = s.setting_name;
-- Preview widgets are simply: SELECT ... FROM settings_latest WHERE is_preview

-- View: Workspace comparison (identifies inconsistencies; category from the map)
CREATE OR REPLACE VIEW main.config_insights.workspace_comparison AS
WITH latest AS (
    SELECT MAX(collected_at) AS max_ts FROM main.config_insights.settings_history
),
cur_cat AS (
    SELECT setting_name, category FROM (
        SELECT setting_name, category,
               ROW_NUMBER() OVER (PARTITION BY setting_name ORDER BY classified_at DESC) AS rn
        FROM main.config_insights.setting_category_map
    ) WHERE rn = 1
),
current_settings AS (
    SELECT s.setting_name, s.setting_value, s.workspace_id,
           COALESCE(cc.category, s.category) AS category
    FROM main.config_insights.settings_history s
    INNER JOIN latest l ON s.collected_at = l.max_ts
    LEFT JOIN cur_cat cc ON cc.setting_name = s.setting_name
    WHERE s.scope = 'workspace'
),
agg AS (
    SELECT setting_name, category,
           COUNT(DISTINCT setting_value) AS distinct_values,
           COUNT(DISTINCT workspace_id) AS workspace_count,
           FIRST(setting_value) AS sample_value
    FROM current_settings
    GROUP BY setting_name, category
)
SELECT a.setting_name, a.category, a.distinct_values, a.workspace_count,
       CASE WHEN a.distinct_values > 1 THEN 'INCONSISTENT' ELSE 'CONSISTENT' END AS consistency_status,
       a.sample_value
FROM agg a;

-- View: Configuration drift (value_changed / added / removed vs previous snapshot)
-- Sentinel values (<unavailable> / <null> / <not-set>) are treated as "no
-- reliable value", so flips to/from a sentinel are not drift. Category comes
-- from the map. Powers the Drift page and the drift_detected alert (which
-- filters to the latest run).
CREATE OR REPLACE VIEW main.config_insights.settings_drift AS
WITH runs AS (
    SELECT collected_at, DENSE_RANK() OVER (ORDER BY collected_at) AS seq
    FROM (SELECT DISTINCT collected_at FROM main.config_insights.settings_history)
),
keys AS (
    SELECT DISTINCT setting_name, COALESCE(workspace_id, 0) AS ws
    FROM main.config_insights.settings_history
),
grid AS (
    SELECT k.setting_name, k.ws, r.collected_at, r.seq FROM keys k CROSS JOIN runs r
),
obs AS (
    SELECT setting_name, COALESCE(workspace_id, 0) AS ws, collected_at,
           MAX(setting_value) AS setting_value, MAX(workspace_name) AS workspace_name
    FROM main.config_insights.settings_history
    GROUP BY setting_name, COALESCE(workspace_id, 0), collected_at
),
matrix AS (
    SELECT g.setting_name, g.ws, g.collected_at, g.seq, o.setting_value, o.workspace_name
    FROM grid g
    LEFT JOIN obs o ON g.setting_name = o.setting_name AND g.ws = o.ws AND g.collected_at = o.collected_at
),
lagged AS (
    SELECT setting_name, ws, collected_at, seq, setting_value, workspace_name,
           LAG(setting_value) OVER (PARTITION BY setting_name, ws ORDER BY seq) AS prev_value,
           LAG(workspace_name) OVER (PARTITION BY setting_name, ws ORDER BY seq) AS prev_ws_name
    FROM matrix
),
cur_cat AS (
    SELECT setting_name, category FROM (
        SELECT setting_name, category,
               ROW_NUMBER() OVER (PARTITION BY setting_name ORDER BY classified_at DESC) AS rn
        FROM main.config_insights.setting_category_map
    ) WHERE rn = 1
)
SELECT DATE(l.collected_at) AS change_date, l.setting_name,
       COALESCE(l.workspace_name, l.prev_ws_name) AS workspace_name,
       cc.category AS category,
       CASE
           WHEN l.prev_value IS NULL AND l.setting_value IS NOT NULL
                AND l.setting_value NOT IN ('<unavailable>', '<null>', '<not-set>') THEN 'added'
           WHEN l.prev_value IS NOT NULL AND l.prev_value NOT IN ('<unavailable>', '<null>', '<not-set>')
                AND l.setting_value IS NULL THEN 'removed'
           ELSE 'value_changed'
       END AS change_type,
       l.prev_value AS previous_value, l.setting_value AS new_value, l.collected_at AS detected_at
FROM lagged l
LEFT JOIN cur_cat cc ON cc.setting_name = l.setting_name
WHERE l.seq > 1 AND (
    (l.prev_value IS NOT NULL AND l.prev_value NOT IN ('<unavailable>', '<null>', '<not-set>')
        AND l.setting_value IS NOT NULL AND l.setting_value NOT IN ('<unavailable>', '<null>', '<not-set>')
        AND l.setting_value <> l.prev_value)
    OR (l.prev_value IS NULL AND l.setting_value IS NOT NULL
        AND l.setting_value NOT IN ('<unavailable>', '<null>', '<not-set>'))
    OR (l.prev_value IS NOT NULL AND l.prev_value NOT IN ('<unavailable>', '<null>', '<not-set>')
        AND l.setting_value IS NULL)
);
