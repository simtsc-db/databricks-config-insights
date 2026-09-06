-- Configuration Insights - Schema Setup (REFERENCE ONLY)
-- Tables and views are created automatically by the collector job; this file
-- documents the schema. All objects live in ONE configurable location:
--   <catalog>.<schema>   (DAB variables `catalog` + `schema`; defaults main.config_insights)
-- Replace `main.config_insights` below with your own catalog.schema if different.
-- Tables use schema evolution: new fields from the API are added automatically.
--
-- Data model (single source of truth per concern):
--   settings_history          append-only snapshot log (one row per setting per run)
--   setting_category_map      setting_name -> functional category (from ai_classify)
--   setting_action_map        setting_name -> audit service/action bridge (attribution)
--   settings_latest           enriched current state (category from the map, is_preview, status)
--   settings_drift            value_changed / added / removed vs the previous snapshot
--   settings_drift_attributed drift rows + best-effort actor from system.access.audit
--   workspace_comparison      cross-workspace consistency
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
           MAX(setting_value) AS setting_value, MAX(workspace_name) AS workspace_name,
           MAX(scope) AS scope, MAX(account_id) AS account_id
    FROM main.config_insights.settings_history
    GROUP BY setting_name, COALESCE(workspace_id, 0), collected_at
),
matrix AS (
    SELECT g.setting_name, g.ws, g.collected_at, g.seq, o.setting_value, o.workspace_name,
           o.scope, o.account_id
    FROM grid g
    LEFT JOIN obs o ON g.setting_name = o.setting_name AND g.ws = o.ws AND g.collected_at = o.collected_at
),
lagged AS (
    SELECT setting_name, ws, collected_at, seq, setting_value, workspace_name, scope, account_id,
           LAG(setting_value) OVER (PARTITION BY setting_name, ws ORDER BY seq) AS prev_value,
           LAG(workspace_name) OVER (PARTITION BY setting_name, ws ORDER BY seq) AS prev_ws_name,
           LAG(scope) OVER (PARTITION BY setting_name, ws ORDER BY seq) AS prev_scope,
           LAG(account_id) OVER (PARTITION BY setting_name, ws ORDER BY seq) AS prev_account_id,
           LAG(collected_at) OVER (PARTITION BY setting_name, ws ORDER BY seq) AS prev_collected_at
    FROM matrix
),
cur_cat AS (
    SELECT setting_name, category FROM (
        SELECT setting_name, category,
               ROW_NUMBER() OVER (PARTITION BY setting_name ORDER BY classified_at DESC) AS rn
        FROM main.config_insights.setting_category_map
    ) WHERE rn = 1
)
-- ws = COALESCE(workspace_id, 0); 0 marks account-scoped drift, which the audit
-- join matches to account-level events (workspace_id = 0). previous_collected_at
-- is the start of the snapshot change-window used for attribution.
SELECT DATE(l.collected_at) AS change_date, l.setting_name,
       l.ws AS workspace_id,
       COALESCE(l.workspace_name, l.prev_ws_name) AS workspace_name,
       COALESCE(l.scope, l.prev_scope) AS scope,
       COALESCE(l.account_id, l.prev_account_id) AS account_id,
       cc.category AS category,
       CASE
           WHEN l.prev_value IS NULL AND l.setting_value IS NOT NULL
                AND l.setting_value NOT IN ('<unavailable>', '<null>', '<not-set>') THEN 'added'
           WHEN l.prev_value IS NOT NULL AND l.prev_value NOT IN ('<unavailable>', '<null>', '<not-set>')
                AND l.setting_value IS NULL THEN 'removed'
           ELSE 'value_changed'
       END AS change_type,
       l.prev_value AS previous_value, l.setting_value AS new_value,
       l.prev_collected_at AS previous_collected_at, l.collected_at AS detected_at
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

-- ===========================================================================
-- Attribution: "who made a configuration change"
--
-- The Settings V2 API carries no actor. Attribution is joined in FROM the Unity
-- Catalog audit system table system.access.audit BY CORRELATION -- it is
-- best-effort, NOT proof. Reading system.access.audit requires a UC SELECT
-- grant on schema system.access (grantable by a metastore admin; INDEPENDENT of
-- account-admin status):
--     GRANT USE CATALOG ON CATALOG system TO `<principal>`;
--     GRANT USE SCHEMA  ON SCHEMA  system.access TO `<principal>`;
--     GRANT SELECT      ON SCHEMA  system.access TO `<principal>`;
-- Account-level events carry workspace_id = 0; retention ~365d; ingestion lags
-- minutes-to-hours. The collector probes access at runtime and, if audit is
-- inaccessible, builds a DEGRADED settings_drift_attributed (NULL actor + an
-- attribution_status of AUDIT_NOT_ACCESSIBLE or ACCOUNT_AUDIT_UNAVAILABLE) so
-- the object always exists.
-- ===========================================================================

-- Optional bridge table: setting_name -> audit service/action. TIGHTENS
-- attribution when a mapping exists; otherwise a workspace time-window match is
-- used. WARNING: action_name / request_params keys are VERSION-SENSITIVE -- the
-- seeds are illustrative and MUST be validated against real events.
CREATE TABLE IF NOT EXISTS main.config_insights.setting_action_map (
    setting_name STRING
        COMMENT 'Settings V2 key (or setting family) this bridge applies to',
    service_name STRING
        COMMENT 'audit.service_name that a change to this setting produces',
    action_name STRING
        COMMENT 'audit.action_name that a change to this setting produces',
    request_param_key STRING
        COMMENT 'request_params key carrying the target (for manual verification)'
)
USING DELTA
COMMENT 'setting_name -> audit service/action bridge. Tightens attribution. VERSION-SENSITIVE: validate against real events.';

-- Illustrative well-known bridges (validate against your account's real events).
MERGE INTO main.config_insights.setting_action_map t
USING (
    SELECT * FROM VALUES
        ('enableIpAccessLists', 'ipAccessLists', 'updateIpAccessList',  'ipAccessListId'),
        ('enableIpAccessLists', 'ipAccessLists', 'createIpAccessList',  'ipAccessListId'),
        ('enableIpAccessLists', 'ipAccessLists', 'replaceIpAccessList', 'ipAccessListId'),
        ('maxTokenLifetimeDays', 'tokens', 'createToken', 'tokenId'),
        ('maxTokenLifetimeDays', 'tokens', 'deleteToken', 'tokenId'),
        ('enableTokensConfig',   'tokens', 'createToken', 'tokenId'),
        ('enableProjectTypeInWorkspace', 'workspace', 'workspaceConfEdit', 'workspaceConfKeys'),
        ('enableExportNotebook',         'workspace', 'workspaceConfEdit', 'workspaceConfKeys')
    AS v(setting_name, service_name, action_name, request_param_key)
) s
ON t.setting_name = s.setting_name AND t.action_name = s.action_name
WHEN NOT MATCHED THEN INSERT *;

-- View: Drift + best-effort actor attribution (ACCESSIBLE form shown here).
-- For each drift row, pick the NEAREST-PRECEDING successful config-related audit
-- event for that workspace inside the change-window (previous_collected_at,
-- detected_at] via ROW_NUMBER() over event_time DESC. Workspace-scoped drift
-- (workspace_id > 0) joins on workspace_id; account-scoped drift (workspace_id =
-- 0) joins on account-level events (workspace_id = 0). setting_action_map
-- tightens the match when a mapping exists; UNMAPPED settings fall back to a
-- workspace time-window match RESTRICTED to config-CHANGE (mutation) actions, so
-- a later read/list event cannot be mis-attributed as the change. Every drift
-- row is preserved (LEFT JOIN) -- a lack of match yields NULL actor +
-- attribution_status NO_AUDIT_MATCH, never a dropped row. attribution_status is
-- one of: ATTRIBUTED (event matched, actor known), ATTRIBUTED_ACTOR_UNKNOWN
-- (event matched but user_identity.email NULL), NO_AUDIT_MATCH,
-- AUDIT_NOT_ACCESSIBLE, ACCOUNT_AUDIT_UNAVAILABLE. This is CORRELATION, not proof.
--
-- When audit is not accessible, the collector instead creates a DEGRADED view:
--   SELECT change_date, setting_name, workspace_id, workspace_name, scope,
--          account_id, category, change_type, previous_value, new_value,
--          previous_collected_at, detected_at,
--          CAST(NULL AS STRING) AS changed_by, CAST(NULL AS TIMESTAMP) AS changed_at,
--          CAST(NULL AS STRING) AS action_name,
--          'AUDIT_NOT_ACCESSIBLE' AS attribution_status   -- or ACCOUNT_AUDIT_UNAVAILABLE
--   FROM main.config_insights.settings_drift;
CREATE OR REPLACE VIEW main.config_insights.settings_drift_attributed AS
WITH audit_events AS (
    SELECT event_time, COALESCE(workspace_id, 0) AS ws, account_id,
           service_name, action_name, user_identity.email AS actor_email
    FROM system.access.audit
    WHERE response.status_code = 200
      AND service_name IN ('workspace', 'accounts', 'unityCatalog', 'ipAccessLists',
                           'tokens', 'settings', 'settingsV2', 'featureStore', 'clusterPolicies')
),
ranked AS (
    SELECT d.*,
           a.actor_email, a.event_time AS audit_event_time, a.action_name AS audit_action_name,
           ROW_NUMBER() OVER (
               PARTITION BY d.setting_name, d.workspace_id, d.detected_at
               ORDER BY a.event_time DESC
           ) AS rn
    FROM main.config_insights.settings_drift d
    LEFT JOIN main.config_insights.setting_action_map m ON m.setting_name = d.setting_name
    LEFT JOIN audit_events a
        ON a.ws = d.workspace_id
        AND d.previous_collected_at IS NOT NULL
        AND a.event_time > d.previous_collected_at
        AND a.event_time <= d.detected_at
        AND (
            -- No mapping: workspace time-window match, RESTRICTED to config-CHANGE
            -- (mutation) actions so a later successful read/list event in the
            -- window cannot outrank (and steal attribution from) the real change.
            (m.action_name IS NULL AND (
                lower(a.action_name) RLIKE '^(create|update|replace|delete|set|add|remove|insert|put|patch|enable|disable|change|edit|grant|revoke|assign|unassign|rotate|generate|register|deregister|attach|detach|move|rename|transfer|reset|upsert)'
                OR lower(a.action_name) LIKE '%edit'
            ))
            -- Mapping exists: tighten to the mapped action (+ service).
            OR (a.action_name = m.action_name
                AND (m.service_name IS NULL OR a.service_name = m.service_name))
        )
)
SELECT change_date, setting_name, workspace_id, workspace_name, scope, account_id,
       category, change_type, previous_value, new_value, previous_collected_at, detected_at,
       CASE WHEN rn = 1 THEN actor_email END AS changed_by,
       CASE WHEN rn = 1 THEN audit_event_time END AS changed_at,
       CASE WHEN rn = 1 THEN audit_action_name END AS action_name,
       -- Distinguish EVENT existence from ACTOR availability: a matched config
       -- event with no actor email is ATTRIBUTED_ACTOR_UNKNOWN (we know a change
       -- happened in-window, we just can't name who), NOT NO_AUDIT_MATCH.
       CASE WHEN rn = 1 AND audit_event_time IS NOT NULL AND actor_email IS NOT NULL THEN 'ATTRIBUTED'
            WHEN rn = 1 AND audit_event_time IS NOT NULL AND actor_email IS NULL THEN 'ATTRIBUTED_ACTOR_UNKNOWN'
            ELSE 'NO_AUDIT_MATCH' END AS attribution_status
FROM ranked
WHERE rn = 1;
