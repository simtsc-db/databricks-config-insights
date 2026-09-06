"""Schema-evolving Delta writer.

Writes collected settings to a Delta table using schema evolution
(mergeSchema=true) so that newly discovered settings or metadata
fields are automatically added as columns without manual DDL changes.

The table keeps a `collected_at` timestamp on every row so that the
dashboard and SQL alerts can compare snapshots and detect drift
(value changes, added/removed settings) with exact SQL.
"""

import logging
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    TimestampType,
)

logger = logging.getLogger(__name__)

SETTINGS_TABLE_SCHEMA = StructType([
    StructField("collected_at", TimestampType(), False),
    StructField("account_id", StringType(), False),
    StructField("scope", StringType(), False),
    StructField("workspace_id", LongType(), True),
    StructField("workspace_name", StringType(), True),
    StructField("setting_name", StringType(), False),
    StructField("setting_value", StringType(), True),
    StructField("setting_type", StringType(), True),
    StructField("source", StringType(), True),
    StructField("category", StringType(), True),
    StructField("preview_phase", StringType(), True),
    StructField("description", StringType(), True),
])


def write_settings(
    spark: SparkSession,
    records: list[dict],
    table_name: str,
) -> None:
    """Write setting records to a Delta table with schema evolution.

    If the table doesn't exist, it is created. If new columns appear
    in the data (e.g., a new metadata field from the API), schema
    evolution adds them automatically.
    """
    if not records:
        logger.warning("No records to write")
        return

    # Parse ISO timestamps back to datetime objects for Spark
    for r in records:
        if isinstance(r.get("collected_at"), str):
            r["collected_at"] = datetime.fromisoformat(r["collected_at"])

    df = spark.createDataFrame(records, schema=SETTINGS_TABLE_SCHEMA)

    df.write.format("delta").mode("append").option(
        "mergeSchema", "true"
    ).saveAsTable(table_name)

    logger.info("Wrote %d records to %s with schema evolution", len(records), table_name)


def ensure_table_properties(spark: SparkSession, table_name: str) -> None:
    """Set Delta table properties.

    - delta.autoOptimize: keeps the table performant
    - delta.enableChangeDataFeed: kept on for any downstream incremental
      consumers (harmless if unused)
    """
    spark.sql(f"""
        ALTER TABLE {table_name}
        SET TBLPROPERTIES (
            'delta.enableChangeDataFeed' = 'true',
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true'
        )
    """)
    logger.info("Table properties set on %s (CDF enabled)", table_name)


# Sentinel values returned when a setting's value cannot be read reliably.
# They are excluded from drift detection (a flip to/from a sentinel is noise).
_SENTINELS = "('<unavailable>', '<null>', '<not-set>')"

# Preview phases that mean "not a preview" (GA / unset).
_NON_PREVIEW = "('GA', 'None', '', 'PreviewPhase.GA')"


def _current_category_cte(map_table: str) -> str:
    """CTE that yields the latest category per setting from the category map.

    Category has a single source of truth (setting_category_map), so every
    view derives it here rather than trusting the point-in-time value stored
    on each historical snapshot row.
    """
    return f"""
        cur_cat AS (
            SELECT setting_name, category FROM (
                SELECT setting_name, category,
                       ROW_NUMBER() OVER (
                           PARTITION BY setting_name ORDER BY classified_at DESC
                       ) AS rn
                FROM {map_table}
            ) WHERE rn = 1
        )
    """


def create_latest_snapshot_view(
    spark: SparkSession,
    table_name: str,
    view_name: str,
    map_table: str,
) -> None:
    """Create the enriched current-state view.

    One view powers the whole Overview page: the latest snapshot of every
    setting, with the category resolved from the category map and two derived
    columns -- ``is_preview`` (non-GA preview phase) and ``status``
    (ENABLED/DISABLED/OTHER). Preview widgets are just ``WHERE is_preview``.
    """
    spark.sql(f"""
        CREATE OR REPLACE VIEW {view_name} AS
        WITH latest AS (
            SELECT MAX(collected_at) AS max_ts FROM {table_name}
        ),
        {_current_category_cte(map_table)}
        SELECT
            s.collected_at, s.account_id, s.scope, s.workspace_id, s.workspace_name,
            s.setting_name, s.setting_value, s.setting_type, s.source,
            COALESCE(cc.category, s.category) AS category,
            s.preview_phase, s.description,
            (s.preview_phase IS NOT NULL
                AND s.preview_phase NOT IN {_NON_PREVIEW}) AS is_preview,
            CASE
                WHEN LOWER(s.setting_value) IN ('true', 'enabled', '1', 'on') THEN 'ENABLED'
                WHEN LOWER(s.setting_value) IN ('false', 'disabled', '0', 'off') THEN 'DISABLED'
                ELSE 'OTHER'
            END AS status
        FROM {table_name} s
        INNER JOIN latest l ON s.collected_at = l.max_ts
        LEFT JOIN cur_cat cc ON cc.setting_name = s.setting_name
    """)
    logger.info("Created latest snapshot view: %s", view_name)


def create_pivot_view(
    spark: SparkSession,
    table_name: str,
    view_name: str,
    map_table: str,
) -> None:
    """Create a workspace comparison pivot view.

    Aggregates settings per workspace to identify inconsistencies. Category
    comes from the category map (single source of truth).
    """
    spark.sql(f"""
        CREATE OR REPLACE VIEW {view_name} AS
        WITH latest AS (
            SELECT MAX(collected_at) AS max_ts FROM {table_name}
        ),
        {_current_category_cte(map_table)},
        current_settings AS (
            SELECT s.setting_name, s.setting_value, s.workspace_id,
                   COALESCE(cc.category, s.category) AS category
            FROM {table_name} s
            INNER JOIN latest l ON s.collected_at = l.max_ts
            LEFT JOIN cur_cat cc ON cc.setting_name = s.setting_name
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
        FROM agg a
    """)
    logger.info("Created pivot comparison view: %s", view_name)


def create_drift_view(
    spark: SparkSession,
    table_name: str,
    view_name: str,
    map_table: str,
) -> None:
    """Create the configuration-drift view.

    Compares each setting to its previous snapshot and classifies every change
    as value_changed / added / removed. Sentinel values (unreadable) are
    treated as "no reliable value", so a flip to/from a sentinel is not drift.
    Category comes from the category map. Powers the Drift page and the
    drift_detected alert (which filters to the latest run).

    Also exposes ``workspace_id`` (0 for account-scoped drift), ``scope``,
    ``account_id`` and ``previous_collected_at`` so ``settings_drift_attributed``
    can correlate each change to an audit event within the snapshot
    change-window ``(previous_collected_at, detected_at]``.
    """
    spark.sql(f"""
        CREATE OR REPLACE VIEW {view_name} AS
        WITH runs AS (
            SELECT collected_at,
                   DENSE_RANK() OVER (ORDER BY collected_at) AS seq
            FROM (SELECT DISTINCT collected_at FROM {table_name})
        ),
        keys AS (
            SELECT DISTINCT setting_name, COALESCE(workspace_id, 0) AS ws FROM {table_name}
        ),
        grid AS (
            SELECT k.setting_name, k.ws, r.collected_at, r.seq
            FROM keys k CROSS JOIN runs r
        ),
        obs AS (
            SELECT setting_name, COALESCE(workspace_id, 0) AS ws, collected_at,
                   MAX(setting_value) AS setting_value,
                   MAX(workspace_name) AS workspace_name,
                   MAX(scope) AS scope,
                   MAX(account_id) AS account_id
            FROM {table_name}
            GROUP BY setting_name, COALESCE(workspace_id, 0), collected_at
        ),
        matrix AS (
            SELECT g.setting_name, g.ws, g.collected_at, g.seq,
                   o.setting_value, o.workspace_name, o.scope, o.account_id
            FROM grid g
            LEFT JOIN obs o ON g.setting_name = o.setting_name
                AND g.ws = o.ws AND g.collected_at = o.collected_at
        ),
        lagged AS (
            SELECT setting_name, ws, collected_at, seq, setting_value, workspace_name,
                   scope, account_id,
                   LAG(setting_value) OVER (
                       PARTITION BY setting_name, ws ORDER BY seq) AS prev_value,
                   LAG(workspace_name) OVER (
                       PARTITION BY setting_name, ws ORDER BY seq) AS prev_ws_name,
                   LAG(scope) OVER (
                       PARTITION BY setting_name, ws ORDER BY seq) AS prev_scope,
                   LAG(account_id) OVER (
                       PARTITION BY setting_name, ws ORDER BY seq) AS prev_account_id,
                   LAG(collected_at) OVER (
                       PARTITION BY setting_name, ws ORDER BY seq) AS prev_collected_at
            FROM matrix
        ),
        {_current_category_cte(map_table)}
        SELECT
            DATE(l.collected_at) AS change_date,
            l.setting_name,
            -- ws is COALESCE(workspace_id, 0); 0 marks account-scoped drift, which
            -- the audit join matches to account-level events (workspace_id = 0).
            l.ws AS workspace_id,
            COALESCE(l.workspace_name, l.prev_ws_name) AS workspace_name,
            COALESCE(l.scope, l.prev_scope) AS scope,
            COALESCE(l.account_id, l.prev_account_id) AS account_id,
            cc.category AS category,
            CASE
                WHEN l.prev_value IS NULL AND l.setting_value IS NOT NULL
                     AND l.setting_value NOT IN {_SENTINELS} THEN 'added'
                WHEN l.prev_value IS NOT NULL AND l.prev_value NOT IN {_SENTINELS}
                     AND l.setting_value IS NULL THEN 'removed'
                ELSE 'value_changed'
            END AS change_type,
            l.prev_value AS previous_value,
            l.setting_value AS new_value,
            -- Start of the snapshot change-window: the previous run's timestamp.
            -- Attribution searches audit events in (previous_collected_at, detected_at].
            l.prev_collected_at AS previous_collected_at,
            l.collected_at AS detected_at
        FROM lagged l
        LEFT JOIN cur_cat cc ON cc.setting_name = l.setting_name
        WHERE l.seq > 1 AND (
            (l.prev_value IS NOT NULL AND l.prev_value NOT IN {_SENTINELS}
                AND l.setting_value IS NOT NULL AND l.setting_value NOT IN {_SENTINELS}
                AND l.setting_value <> l.prev_value)
            OR (l.prev_value IS NULL AND l.setting_value IS NOT NULL
                AND l.setting_value NOT IN {_SENTINELS})
            OR (l.prev_value IS NOT NULL AND l.prev_value NOT IN {_SENTINELS}
                AND l.setting_value IS NULL)
        )
    """)
    logger.info("Created drift view: %s", view_name)


# --------------------------------------------------------------------------- #
# Attribution: "who made a configuration change".
#
# The Settings V2 API carries no actor. Attribution is joined in from the Unity
# Catalog audit system table system.access.audit BY CORRELATION -- for each
# drift row we pick the nearest-preceding successful config-related audit event
# for that workspace inside the snapshot change-window. It is a best-effort
# correlation, NOT proof: many actors and API calls can touch a workspace inside
# one collection interval, and audit ingestion lags minutes-to-hours. Never drop
# a drift row for lack of a match -- unmatched rows are labelled, not removed.
# --------------------------------------------------------------------------- #

AUDIT_TABLE = "system.access.audit"

# Attribution status enum (also surfaced on the dashboard/alert). Note the
# distinction between EVENT existence and ACTOR availability:
ATTR_ATTRIBUTED = "ATTRIBUTED"                        # matching event found AND actor email present
ATTR_ACTOR_UNKNOWN = "ATTRIBUTED_ACTOR_UNKNOWN"       # matching event found but user_identity.email is NULL
ATTR_NO_AUDIT_MATCH = "NO_AUDIT_MATCH"                # audit readable, but no event matched
ATTR_NOT_ACCESSIBLE = "AUDIT_NOT_ACCESSIBLE"          # missing SELECT on system.access
ATTR_ACCOUNT_UNAVAILABLE = "ACCOUNT_AUDIT_UNAVAILABLE"  # audit schema not enabled / not found

# Audit modes returned by the probe -> attribution behaviour.
AUDIT_MODE_ACCESSIBLE = "ACCESSIBLE"

# Coarse pre-filter for config-related audit events. Kept deliberately broad
# (surfaces that carry configuration changes); setting_action_map TIGHTENS this
# per-setting when a mapping exists. These service/action names are
# VERSION-SENSITIVE and must be validated against real events in your account.
_CONFIG_AUDIT_SERVICES = (
    "('workspace', 'accounts', 'unityCatalog', 'ipAccessLists', "
    "'tokens', 'settings', 'settingsV2', 'featureStore', 'clusterPolicies')"
)

# Config-CHANGE (mutation) action allowlist for the UNMAPPED fallback. Without a
# setting_action_map row we would otherwise accept ANY successful event from a
# config service, letting a later READ/LIST event inside the change-window
# outrank the real change (ROW_NUMBER over event_time DESC) and wrongly
# attribute the reader. This restricts unmapped fallback candidates to mutating
# actions (create/update/replace/delete/set/.../*edit). When a setting_action_map
# row exists we use its specific action instead, so this guard is fallback-only.
# `a` is the audit_events alias in the attributed view.
_MUTATION_ACTION_PREDICATE = (
    "(lower(a.action_name) RLIKE "
    "'^(create|update|replace|delete|set|add|remove|insert|put|patch|enable|"
    "disable|change|edit|grant|revoke|assign|unassign|rotate|generate|register|"
    "deregister|attach|detach|move|rename|transfer|reset|upsert)' "
    "OR lower(a.action_name) LIKE '%edit')"
)


def classify_audit_error(exc: Exception) -> str:
    """Map an audit-access failure to the right attribution status.

    Distinguishes a permission problem (missing SELECT on ``system.access`` ->
    ``ATTR_NOT_ACCESSIBLE``) from the audit schema not being enabled/found
    (``ATTR_ACCOUNT_UNAVAILABLE``). Unknown failures degrade to
    ``ATTR_NOT_ACCESSIBLE``.
    """
    msg = str(exc).upper()
    not_found_markers = (
        "TABLE_OR_VIEW_NOT_FOUND",
        "SCHEMA_NOT_FOUND",
        "NAMESPACE_NOT_FOUND",
        "DOES NOT EXIST",
        "CANNOT BE FOUND",
        "NOT FOUND",
    )
    perm_markers = (
        "PERMISSION",
        "INSUFFICIENT_PRIVILEGES",
        "INSUFFICIENT PRIVILEGES",
        "ACCESS DENIED",
        "NOT AUTHORIZED",
        "REQUIRES",
        "UNAUTHORIZED",
    )
    if any(m in msg for m in not_found_markers):
        return ATTR_ACCOUNT_UNAVAILABLE
    if any(m in msg for m in perm_markers):
        return ATTR_NOT_ACCESSIBLE
    return ATTR_NOT_ACCESSIBLE


def probe_audit_access(spark: SparkSession) -> str:
    """Cheap, bounded probe of read access to ``system.access.audit``.

    Returns one of ``AUDIT_MODE_ACCESSIBLE`` / ``ATTR_NOT_ACCESSIBLE`` /
    ``ATTR_ACCOUNT_UNAVAILABLE``. Never raises: any unexpected failure degrades
    to ``ATTR_NOT_ACCESSIBLE`` so the collector job can still build a (degraded)
    attributed view instead of failing.

    Distinguishes a permission problem (a SELECT grant on schema
    ``system.access`` is missing -- grantable by a metastore admin, independent
    of account-admin status) from the audit schema not being enabled at all
    (table/schema not found).
    """
    try:
        spark.sql(
            f"SELECT 1 FROM {AUDIT_TABLE} "
            "WHERE event_date >= current_date() LIMIT 1"
        ).collect()
        logger.info("Audit probe: %s is readable", AUDIT_TABLE)
        return AUDIT_MODE_ACCESSIBLE
    except Exception as e:  # noqa: BLE001 - must never fail the job
        status = classify_audit_error(e)
        if status == ATTR_ACCOUNT_UNAVAILABLE:
            logger.warning(
                "Audit probe: %s not found -> audit schema not enabled (%s)",
                AUDIT_TABLE, e,
            )
        else:
            logger.warning(
                "Audit probe: system.access not readable (%s). Grant "
                "USE CATALOG ON system, USE SCHEMA + SELECT ON SCHEMA "
                "system.access (metastore admin) to enable attribution.",
                e,
            )
        return status


def ensure_setting_action_map(spark: SparkSession, action_map_table: str) -> None:
    """Create and seed the optional ``setting_action_map`` bridge table.

    Maps a ``setting_name`` to the audit ``service_name`` / ``action_name``
    (and the ``request_params`` key that carries the target) that a change to
    that setting produces. Used only to TIGHTEN attribution: when a mapping
    exists for a setting, only audit events with the mapped action_name are
    considered; otherwise attribution falls back to a workspace time-window
    match. Rows with no matching drift setting_name are simply never used, so an
    inaccurate guess is harmless (it just never tightens).

    WARNING: action_name / request_params keys are VERSION-SENSITIVE. The seeds
    below are illustrative well-known bridges and MUST be validated against real
    events in ``system.access.audit`` for your Databricks version before you
    rely on them.
    """
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {action_map_table} (
            setting_name STRING
                COMMENT 'Settings V2 key (or setting family) this bridge applies to',
            service_name STRING
                COMMENT 'audit.service_name that a change to this setting produces',
            action_name STRING
                COMMENT 'audit.action_name that a change to this setting produces',
            request_param_key STRING
                COMMENT 'request_params key carrying the target (for manual verification)'
        ) USING delta
        COMMENT 'setting_name -> audit service/action bridge. Tightens attribution. VERSION-SENSITIVE: validate against real events.'
        """
    )

    # Idempotent seed of well-known bridges. These are EXAMPLES to be validated;
    # setting_name values in particular vary by API version.
    seeds = [
        # IP access lists (workspace network config)
        ("enableIpAccessLists", "ipAccessLists", "updateIpAccessList", "ipAccessListId"),
        ("enableIpAccessLists", "ipAccessLists", "createIpAccessList", "ipAccessListId"),
        ("enableIpAccessLists", "ipAccessLists", "replaceIpAccessList", "ipAccessListId"),
        # Personal access tokens
        ("maxTokenLifetimeDays", "tokens", "createToken", "tokenId"),
        ("maxTokenLifetimeDays", "tokens", "deleteToken", "tokenId"),
        ("enableTokensConfig", "tokens", "createToken", "tokenId"),
        # Generic Settings V2 workspace toggles -> workspaceConfEdit
        ("enableProjectTypeInWorkspace", "workspace", "workspaceConfEdit", "workspaceConfKeys"),
        ("enableExportNotebook", "workspace", "workspaceConfEdit", "workspaceConfKeys"),
    ]
    values_sql = ",\n            ".join(
        "('{}', '{}', '{}', '{}')".format(
            s.replace("'", "''"), sv.replace("'", "''"),
            a.replace("'", "''"), k.replace("'", "''"),
        )
        for s, sv, a, k in seeds
    )
    spark.sql(
        f"""
        MERGE INTO {action_map_table} t
        USING (
            SELECT * FROM VALUES
            {values_sql}
            AS v(setting_name, service_name, action_name, request_param_key)
        ) s
        ON t.setting_name = s.setting_name AND t.action_name = s.action_name
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    logger.info("Ensured + seeded setting_action_map: %s", action_map_table)


def create_drift_attributed_view(
    spark: SparkSession,
    drift_view: str,
    action_map_table: str,
    view_name: str,
    audit_mode: str,
) -> None:
    """Create ``settings_drift_attributed``: drift rows + best-effort actor.

    When ``audit_mode`` is ``ACCESSIBLE`` the view LEFT JOINs each drift row to
    ``system.access.audit`` and, per (setting, workspace, snapshot), keeps the
    NEAREST-PRECEDING successful (response.status_code = 200) config-related
    event inside the change-window ``(previous_collected_at, detected_at]`` via
    ROW_NUMBER() over event_time DESC. Workspace-scoped drift (workspace_id > 0)
    joins on that workspace_id; account-scoped drift (workspace_id = 0) joins on
    account-level events (workspace_id = 0). ``setting_action_map`` tightens the
    match when a mapping exists; UNMAPPED settings fall back to a workspace
    time-window match RESTRICTED to config-CHANGE (mutation) actions, so a later
    successful read/list event cannot be mis-attributed as the change. A matched
    event with no actor email yields ``ATTRIBUTED_ACTOR_UNKNOWN`` (not
    ``NO_AUDIT_MATCH``): we know a change happened in-window but cannot name who.

    When audit is NOT accessible the view degrades to the drift rows plus NULL
    ``changed_by`` / ``changed_at`` / ``action_name`` and a constant
    ``attribution_status`` (so the object always builds and the dashboard/alert
    keep working). Every drift row is preserved regardless of match.

    Attribution is correlation, NOT proof.
    """
    if audit_mode != AUDIT_MODE_ACCESSIBLE:
        # Degraded form: never references system.access.audit, so it builds and
        # queries fine without the grant. attribution_status explains why.
        spark.sql(f"""
            CREATE OR REPLACE VIEW {view_name} AS
            -- Attribution unavailable ({audit_mode}); drift rows carry NULL actor.
            -- Attribution is best-effort correlation with system.access.audit,
            -- NOT proof of who made the change.
            SELECT
                change_date, setting_name, workspace_id, workspace_name, scope,
                account_id, category, change_type, previous_value, new_value,
                previous_collected_at, detected_at,
                CAST(NULL AS STRING) AS changed_by,
                CAST(NULL AS TIMESTAMP) AS changed_at,
                CAST(NULL AS STRING) AS action_name,
                '{audit_mode}' AS attribution_status
            FROM {drift_view}
        """)
        logger.info(
            "Created attributed view (degraded, status=%s): %s",
            audit_mode, view_name,
        )
        return

    spark.sql(f"""
        CREATE OR REPLACE VIEW {view_name} AS
        -- Attribution is best-effort CORRELATION with system.access.audit, NOT
        -- proof: for each drift row we pick the nearest-preceding successful
        -- config-related audit event for that workspace inside the change-window
        -- (previous_collected_at, detected_at]. setting_action_map tightens the
        -- match when a mapping exists; otherwise a workspace time-window match is
        -- used. Every drift row is preserved (LEFT JOIN); no match -> NULL actor.
        WITH audit_events AS (
            SELECT
                event_time,
                COALESCE(workspace_id, 0) AS ws,
                account_id,
                service_name,
                action_name,
                user_identity.email AS actor_email
            FROM {AUDIT_TABLE}
            WHERE response.status_code = 200
              AND service_name IN {_CONFIG_AUDIT_SERVICES}
        ),
        ranked AS (
            SELECT
                d.*,
                a.actor_email, a.event_time AS audit_event_time,
                a.action_name AS audit_action_name,
                ROW_NUMBER() OVER (
                    PARTITION BY d.setting_name, d.workspace_id, d.detected_at
                    ORDER BY a.event_time DESC
                ) AS rn
            FROM {drift_view} d
            LEFT JOIN {action_map_table} m
                ON m.setting_name = d.setting_name
            LEFT JOIN audit_events a
                ON a.ws = d.workspace_id
                AND d.previous_collected_at IS NOT NULL
                AND a.event_time > d.previous_collected_at
                AND a.event_time <= d.detected_at
                AND (
                    -- No mapping for this setting: workspace time-window match,
                    -- RESTRICTED to config-CHANGE (mutation) actions so a later
                    -- successful read/list event in the window cannot outrank
                    -- (and steal attribution from) the real change.
                    (m.action_name IS NULL AND {_MUTATION_ACTION_PREDICATE})
                    -- Mapping exists: tighten to the mapped action (+ service).
                    OR (a.action_name = m.action_name
                        AND (m.service_name IS NULL
                             OR a.service_name = m.service_name))
                )
        )
        SELECT
            change_date, setting_name, workspace_id, workspace_name, scope,
            account_id, category, change_type, previous_value, new_value,
            previous_collected_at, detected_at,
            CASE WHEN rn = 1 THEN actor_email END AS changed_by,
            CASE WHEN rn = 1 THEN audit_event_time END AS changed_at,
            CASE WHEN rn = 1 THEN audit_action_name END AS action_name,
            -- Distinguish EVENT existence from ACTOR availability. A matched
            -- config event (audit_event_time IS NOT NULL) with no actor email
            -- is ATTRIBUTED_ACTOR_UNKNOWN, NOT NO_AUDIT_MATCH -- we know a change
            -- happened in-window, we just can't name the actor. No matched event
            -- (LEFT JOIN produced NULLs) is NO_AUDIT_MATCH.
            CASE
                WHEN rn = 1 AND audit_event_time IS NOT NULL
                     AND actor_email IS NOT NULL THEN '{ATTR_ATTRIBUTED}'
                WHEN rn = 1 AND audit_event_time IS NOT NULL
                     AND actor_email IS NULL THEN '{ATTR_ACTOR_UNKNOWN}'
                ELSE '{ATTR_NO_AUDIT_MATCH}'
            END AS attribution_status
        FROM ranked
        WHERE rn = 1
    """)
    logger.info("Created attributed view (audit accessible): %s", view_name)
