"""Entry point for the Config Insights collection job.

Designed to run as a Databricks serverless job task. Uses:
- AccountClient for cross-workspace discovery
- SparkSession for Delta writes with schema evolution

Settings are classified into functional categories via ai_classify. Drift
detection is handled downstream by the dashboard and SQL alerts via exact
snapshot-to-snapshot comparison of settings_history.

Environment variables / job parameters:
  --catalog / CONFIG_CATALOG        Output catalog (default: config_insights)
  --schema / CONFIG_SCHEMA          Output schema (default: default)
  --account-id / DATABRICKS_ACCOUNT_ID
                                    Account ID for cross-workspace scanning;
                                    "none"/empty ⇒ workspace-only mode
  --categories / CONFIG_CATEGORIES  Comma-separated functional categories used
                                    by ai_classify (preview is NOT a category)
"""

import argparse
import logging
import os
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("config_insights")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Databricks Configuration Insights - Dynamic Collector"
    )
    parser.add_argument(
        "--catalog",
        default=os.environ.get("CONFIG_CATALOG", "config_insights"),
    )
    parser.add_argument(
        "--schema",
        default=os.environ.get("CONFIG_SCHEMA", "default"),
    )
    parser.add_argument(
        "--account-id",
        default=os.environ.get("DATABRICKS_ACCOUNT_ID"),
        help="Databricks account ID for cross-workspace scanning",
    )
    parser.add_argument(
        "--workspace-ids",
        default=None,
        help="Comma-separated workspace IDs to scan (default: all)",
    )
    parser.add_argument(
        "--categories",
        default=os.environ.get(
            "CONFIG_CATEGORIES",
            "governance,ingestion,AI,ML,compute,marketplace,platform,other",
        ),
        help="Comma-separated functional categories for ai_classify",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Collect and print settings without writing to Delta",
    )
    args = parser.parse_args()

    catalog = args.catalog
    schema = args.schema
    categories = [c.strip() for c in args.categories.split(",") if c.strip()]
    table_name = f"{catalog}.{schema}.settings_history"
    view_latest = f"{catalog}.{schema}.settings_latest"
    view_comparison = f"{catalog}.{schema}.workspace_comparison"

    # Parse workspace filter
    workspace_filter = None
    if args.workspace_ids:
        workspace_filter = [
            int(ws.strip()) for ws in args.workspace_ids.split(",")
        ]

    # Initialize clients
    from databricks.sdk import AccountClient, WorkspaceClient
    from config_insights.collector import ConfigInsightsCollector

    records = None

    # Normalize the account id: an empty string or the "none" sentinel (the
    # DAB default) means workspace-only mode. A non-empty string in the job's
    # parameter list is required because Terraform rejects null list items.
    account_id_arg = args.account_id
    if account_id_arg and account_id_arg.strip().lower() in ("", "none"):
        account_id_arg = None

    # Set account ID from CLI arg so AccountClient picks it up
    if account_id_arg:
        os.environ["DATABRICKS_ACCOUNT_ID"] = account_id_arg

    # Try account-level collection first (requires account admin credentials)
    if account_id_arg:
        try:
            account_client = AccountClient(account_id=account_id_arg)
            logger.info("Connected to account: %s", account_client.config.account_id)
            collector = ConfigInsightsCollector(
                account_client=account_client,
                workspace_filter=workspace_filter,
            )
            records = collector.collect_all()
        except Exception as e:
            logger.warning(
                "Account-level collection failed (%s). "
                "Falling back to workspace-only mode.",
                e,
            )

    if records is None:
        # Workspace-only mode: collect settings for the current workspace
        from config_insights.discovery import discover_workspace_settings
        from databricks.sdk.service.provisioning import Workspace
        from datetime import datetime, timezone as tz

        ws_client = WorkspaceClient()

        # Resolve workspace ID (try multiple sources)
        workspace_id = 0
        try:
            workspace_id = int(ws_client.get_workspace_id())
        except Exception:
            pass
        if workspace_id == 0:
            try:
                from pyspark.sql import SparkSession as _Spark
                _s = _Spark.builder.getOrCreate()
                workspace_id = int(_s.conf.get("spark.databricks.clusterUsageTags.orgId", "0"))
            except Exception:
                pass
        if workspace_id == 0:
            try:
                workspace_id = int(os.environ.get("DATABRICKS_WORKSPACE_ID", "0"))
            except (ValueError, TypeError):
                pass

        # Resolve workspace name (try deployment name from status API)
        workspace_name = ws_client.config.host.replace("https://", "").split(".")[0]
        try:
            status = ws_client.workspace.get_status("/")
            deployment_name = ws_client.config.host.replace("https://", "").replace(".cloud.databricks.com", "")
            if deployment_name:
                workspace_name = deployment_name
        except Exception:
            pass

        ws_obj = Workspace(
            workspace_id=workspace_id,
            workspace_name=workspace_name,
        )
        collected_at = datetime.now(tz.utc)
        account_id = account_id_arg or "unknown"

        logger.info("Workspace-only mode: scanning %s (ID: %s)", workspace_name, workspace_id)
        records = discover_workspace_settings(
            ws_client, ws_obj, collected_at, account_id
        )

    if args.dry_run:
        logger.info("DRY RUN: %d records collected", len(records))
        for r in records[:20]:
            logger.info(
                "  [%s] %s/%s = %s",
                r["scope"],
                r.get("workspace_name", "account"),
                r["setting_name"],
                r["setting_value"][:80],
            )
        if len(records) > 20:
            logger.info("  ... and %d more", len(records) - 20)
        return 0

    # Write to Delta with schema evolution
    from pyspark.sql import SparkSession
    from config_insights.categorize import classify_settings
    from config_insights.writer import (
        write_settings,
        ensure_table_properties,
        create_latest_snapshot_view,
        create_pivot_view,
        create_drift_view,
    )

    spark = SparkSession.builder.getOrCreate()

    # Ensure schema exists (catalog must already exist)
    spark.sql(f"USE CATALOG {catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

    # Classify each setting into a functional category via ai_classify
    # (cached in setting_category_map). Preview/lifecycle stays separate.
    classify_settings(spark, records, catalog, schema, categories)

    # Write with schema evolution (new settings = new rows, not columns)
    write_settings(spark, records, table_name)

    # Keep the table performant (auto-optimize); CDF stays enabled for any
    # downstream incremental consumers.
    ensure_table_properties(spark, table_name)

    # Create convenience views. Category is resolved from the category map
    # (single source of truth) inside each view, so they never go stale.
    map_table = f"{catalog}.{schema}.setting_category_map"
    view_drift = f"{catalog}.{schema}.settings_drift"
    create_latest_snapshot_view(spark, table_name, view_latest, map_table)
    create_pivot_view(spark, table_name, view_comparison, map_table)
    create_drift_view(spark, table_name, view_drift, map_table)

    logger.info("Collection complete: %d settings written to %s", len(records), table_name)

    return 0


if __name__ == "__main__":
    sys.exit(main())
