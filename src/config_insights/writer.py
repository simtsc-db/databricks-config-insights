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


def create_latest_snapshot_view(
    spark: SparkSession,
    table_name: str,
    view_name: str,
) -> None:
    """Create a view showing only the most recent collection snapshot.

    This is useful for dashboards that need the current state.
    """
    spark.sql(f"""
        CREATE OR REPLACE VIEW {view_name} AS
        WITH latest AS (
            SELECT MAX(collected_at) AS max_ts FROM {table_name}
        )
        SELECT s.*
        FROM {table_name} s
        INNER JOIN latest l ON s.collected_at = l.max_ts
    """)
    logger.info("Created latest snapshot view: %s", view_name)


def create_pivot_view(
    spark: SparkSession,
    table_name: str,
    view_name: str,
) -> None:
    """Create a workspace comparison pivot view.

    Aggregates settings per workspace to identify inconsistencies.
    """
    spark.sql(f"""
        CREATE OR REPLACE VIEW {view_name} AS
        WITH latest AS (
            SELECT MAX(collected_at) AS max_ts FROM {table_name}
        ),
        current_settings AS (
            SELECT s.*
            FROM {table_name} s
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
        FROM agg a
    """)
    logger.info("Created pivot comparison view: %s", view_name)


def create_heatmap_view(
    spark: SparkSession,
    table_name: str,
    view_name: str,
) -> None:
    """Create a preview features heatmap view.

    Produces a row per (feature, workspace) with a normalized status
    column suitable for heatmap-style visualization in the dashboard.
    The view includes all preview-phase settings and normalizes their
    values to ENABLED/DISABLED/OTHER for consistent display.
    """
    spark.sql(f"""
        CREATE OR REPLACE VIEW {view_name} AS
        WITH latest AS (
            SELECT MAX(collected_at) AS max_ts FROM {table_name}
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
        FROM {table_name} s
        INNER JOIN latest l ON s.collected_at = l.max_ts
        WHERE s.preview_phase IS NOT NULL
          AND s.preview_phase NOT IN ('GA', 'None', '', 'PreviewPhase.GA')
          AND s.scope = 'workspace'
        ORDER BY s.setting_name, s.workspace_name
    """)
    logger.info("Created heatmap view: %s", view_name)
