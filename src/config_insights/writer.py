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
                   MAX(workspace_name) AS workspace_name
            FROM {table_name}
            GROUP BY setting_name, COALESCE(workspace_id, 0), collected_at
        ),
        matrix AS (
            SELECT g.setting_name, g.ws, g.collected_at, g.seq,
                   o.setting_value, o.workspace_name
            FROM grid g
            LEFT JOIN obs o ON g.setting_name = o.setting_name
                AND g.ws = o.ws AND g.collected_at = o.collected_at
        ),
        lagged AS (
            SELECT setting_name, ws, collected_at, seq, setting_value, workspace_name,
                   LAG(setting_value) OVER (
                       PARTITION BY setting_name, ws ORDER BY seq) AS prev_value,
                   LAG(workspace_name) OVER (
                       PARTITION BY setting_name, ws ORDER BY seq) AS prev_ws_name
            FROM matrix
        ),
        {_current_category_cte(map_table)}
        SELECT
            DATE(l.collected_at) AS change_date,
            l.setting_name,
            COALESCE(l.workspace_name, l.prev_ws_name) AS workspace_name,
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
