"""Core collection orchestrator.

Design principles:
- ZERO hardcoded setting keys: all settings are dynamically discovered
  via the Settings V2 list_*_settings_metadata() endpoints.
- Schema evolution: new settings discovered in future runs are automatically
  added as columns to the pivoted snapshot table.
- Drift detection is handled downstream in SQL (dashboard + alerts) via exact
  snapshot-to-snapshot comparison.
"""

import logging
from datetime import datetime, timezone
from typing import Optional

from databricks.sdk import AccountClient, WorkspaceClient
from databricks.sdk.service.provisioning import Workspace

from config_insights.discovery import (
    discover_account_settings,
    discover_workspace_settings,
)

logger = logging.getLogger(__name__)


class ConfigInsightsCollector:
    """Orchestrates dynamic configuration collection across an account."""

    def __init__(
        self,
        account_client: AccountClient,
        workspace_filter: Optional[list[int]] = None,
    ):
        self.account_client = account_client
        self.workspace_filter = workspace_filter

    def collect_all(self) -> list[dict]:
        """Execute a full collection run. Returns flat list of setting records.

        Each record is a dict with:
          collected_at, account_id, scope, workspace_id, workspace_name,
          setting_name, setting_value, setting_type, source, category,
          preview_phase, description
        """
        collected_at = datetime.now(timezone.utc)
        account_id = self.account_client.config.account_id
        all_records: list[dict] = []

        # 1. Account-level settings (best-effort, API may not be available)
        logger.info("Discovering account-level settings...")
        try:
            account_records = discover_account_settings(
                self.account_client, collected_at
            )
            all_records.extend(account_records)
            logger.info("Collected %d account-level settings", len(account_records))
        except Exception as e:
            logger.warning("Account-level settings discovery failed: %s", e)

        # 2. Enumerate workspaces (requires account admin)
        workspaces = self._get_target_workspaces()
        if not workspaces:
            raise RuntimeError(
                "No workspaces found. Ensure the account client has "
                "account admin permissions to list workspaces."
            )
        logger.info("Found %d workspaces to scan", len(workspaces))

        # 3. Iterate workspaces
        for ws in workspaces:
            logger.info(
                "Scanning workspace: %s (ID: %s)",
                ws.workspace_name,
                ws.workspace_id,
            )
            try:
                ws_client = self.account_client.get_workspace_client(ws)
                ws_records = discover_workspace_settings(
                    ws_client, ws, collected_at, account_id
                )
                all_records.extend(ws_records)
                logger.info(
                    "Collected %d settings from %s",
                    len(ws_records),
                    ws.workspace_name,
                )
            except Exception as e:
                logger.error(
                    "Failed to scan workspace %s: %s",
                    ws.workspace_name,
                    e,
                )

        logger.info("Total: %d settings collected", len(all_records))
        return all_records

    def _get_target_workspaces(self) -> list[Workspace]:
        try:
            all_ws = list(self.account_client.workspaces.list())
        except Exception as e:
            logger.warning("Cannot list workspaces via account API: %s", e)
            return []
        if self.workspace_filter:
            return [
                ws for ws in all_ws
                if ws.workspace_id in self.workspace_filter
            ]
        return all_ws
