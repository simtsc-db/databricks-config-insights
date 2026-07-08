"""Lakehouse Monitoring setup for automatic drift detection.

We use a **TimeSeries** profile (not Snapshot) because:

1. Our table is append-only with a `collected_at` timestamp per collection run.
   Each run appends a full snapshot of settings as new rows.
2. TimeSeries computes **consecutive drift** -- comparing today's window to
   yesterday's -- which is exactly "what changed since last run?"
3. TimeSeries supports **incremental processing** via CDF, so only new rows
   are processed on each refresh (cost-efficient at scale).
4. Snapshot would reprocess the entire table on every refresh and does NOT
   support consecutive window comparison (only baseline drift).

The monitor computes:
- Chi-squared tests on categorical columns (setting_value distribution changes)
- Profile metrics (null counts, distinct counts, frequency distributions)
- Drift metrics sliced by scope, category, and workspace_name

The drift metrics table is then used by SQL Alerts to notify on changes.
"""

import logging

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import MonitorTimeSeries, MonitorCronSchedule

logger = logging.getLogger(__name__)


def setup_lakehouse_monitor(
    ws_client: WorkspaceClient,
    table_name: str,
    output_schema: str,
    assets_dir: str,
    schedule_cron: str = "0 30 6 * * ?",
    baseline_table: str | None = None,
) -> dict:
    """Create or update a Lakehouse Monitor on the settings table.

    Uses TimeSeries profile with `collected_at` as the timestamp column.
    This automatically computes:
    - Profile metrics: distribution stats for setting_value per time window
    - Drift metrics: statistical comparison between consecutive windows

    The drift metrics table can be used to create SQL Alerts that fire
    when configuration values change unexpectedly.

    Args:
        ws_client: Authenticated WorkspaceClient
        table_name: Full 3-level name of the settings table
        output_schema: Schema for metric tables (e.g., "config_insights.monitoring")
        assets_dir: Workspace path for monitoring assets/dashboard
        schedule_cron: Quartz cron for monitor refresh (default: 8am daily)
        baseline_table: Optional baseline table for drift comparison

    Returns:
        Dict with monitor info including metric table names
    """
    # Check if monitor already exists
    try:
        existing = ws_client.quality_monitors.get(table_name=table_name)
        logger.info("Monitor already exists for %s, updating...", table_name)
        return _update_monitor(
            ws_client, table_name, output_schema, assets_dir,
            schedule_cron, baseline_table
        )
    except Exception:
        pass

    logger.info("Creating Lakehouse Monitor on %s", table_name)

    try:
        monitor_info = ws_client.quality_monitors.create(
            table_name=table_name,
            assets_dir=assets_dir,
            output_schema_name=output_schema,
            time_series=MonitorTimeSeries(
                timestamp_col="collected_at",
                granularities=["1 day"],
            ),
            schedule=MonitorCronSchedule(
                quartz_cron_expression=schedule_cron,
                timezone_id="UTC",
            ),
            baseline_table_name=baseline_table,
            slicing_exprs=[
                "scope",
                "category",
                "workspace_name",
            ],
        )

        result = {
            "table_name": table_name,
            "profile_metrics_table": monitor_info.profile_metrics_table_name,
            "drift_metrics_table": monitor_info.drift_metrics_table_name,
            "dashboard_id": getattr(monitor_info, "dashboard_id", None),
            "status": str(monitor_info.status),
        }

        logger.info(
            "Monitor created. Drift metrics: %s, Profile metrics: %s",
            result["drift_metrics_table"],
            result["profile_metrics_table"],
        )
        return result

    except Exception as e:
        logger.error("Failed to create Lakehouse Monitor: %s", e)
        raise


def _update_monitor(
    ws_client: WorkspaceClient,
    table_name: str,
    output_schema: str,
    assets_dir: str,
    schedule_cron: str,
    baseline_table: str | None,
) -> dict:
    """Update an existing monitor's configuration."""
    try:
        monitor_info = ws_client.quality_monitors.update(
            table_name=table_name,
            output_schema_name=output_schema,
            time_series=MonitorTimeSeries(
                timestamp_col="collected_at",
                granularities=["1 day"],
            ),
            schedule=MonitorCronSchedule(
                quartz_cron_expression=schedule_cron,
                timezone_id="UTC",
            ),
            baseline_table_name=baseline_table,
            slicing_exprs=[
                "scope",
                "category",
                "workspace_name",
            ],
        )
        return {
            "table_name": table_name,
            "profile_metrics_table": monitor_info.profile_metrics_table_name,
            "drift_metrics_table": monitor_info.drift_metrics_table_name,
            "dashboard_id": getattr(monitor_info, "dashboard_id", None),
            "status": str(monitor_info.status),
        }
    except Exception as e:
        logger.error("Failed to update monitor: %s", e)
        raise


def refresh_monitor(ws_client: WorkspaceClient, table_name: str) -> None:
    """Trigger an immediate refresh of the monitor metrics."""
    try:
        ws_client.quality_monitors.run_refresh(table_name=table_name)
        logger.info("Monitor refresh triggered for %s", table_name)
    except Exception as e:
        logger.warning("Could not trigger monitor refresh: %s", e)
