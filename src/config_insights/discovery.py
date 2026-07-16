"""Dynamic settings discovery via Settings V2 metadata API.

The Settings V2 API is self-describing: list_*_settings_metadata() returns
ALL available setting keys, their types, descriptions, preview phases, and
documentation links. This eliminates the need for hardcoded key lists.

For workspace-conf keys not yet migrated to V2, we discover them by calling
list_workspace_settings_metadata() which also returns workspace_conf keys
that have been onboarded to the V2 metadata surface.

This approach means:
- New settings added by Databricks are automatically discovered on next run
- Preview features with a preview_phase field are captured without manual curation
- No maintenance burden when Databricks adds/removes/renames settings
"""

import logging
from datetime import datetime
from typing import Optional

from databricks.sdk import AccountClient, WorkspaceClient
from databricks.sdk.service.provisioning import Workspace

logger = logging.getLogger(__name__)


def discover_account_settings(
    account_client: AccountClient,
    collected_at: datetime,
) -> list[dict]:
    """Dynamically discover and collect ALL account-level settings.

    Uses list_account_settings_metadata() to enumerate every available
    setting, then retrieves each value via get_public_account_setting().
    """
    records: list[dict] = []
    account_id = account_client.config.account_id

    try:
        metadata_list = list(
            account_client.settings_v2.list_account_settings_metadata(
                page_size=1000
            )
        )
    except Exception as e:
        logger.error("Cannot list account settings metadata: %s", e)
        return records

    logger.info(
        "Discovered %d account-level setting definitions", len(metadata_list)
    )

    for meta in metadata_list:
        name = meta.name
        value = _safe_get_account_setting(account_client, name)

        records.append(
            _build_record(
                collected_at=collected_at,
                account_id=account_id,
                scope="account",
                workspace_id=None,
                workspace_name=None,
                setting_name=name,
                setting_value=value,
                setting_type=getattr(meta, "type", "unknown"),
                source="settings_v2",
                category=None,
                preview_phase=_get_preview_phase(meta),
                description=getattr(meta, "description", None),
            )
        )

    return records


def discover_workspace_settings(
    ws_client: WorkspaceClient,
    workspace: Workspace,
    collected_at: datetime,
    account_id: str,
) -> list[dict]:
    """Dynamically discover and collect ALL workspace-level settings.

    Uses list_workspace_settings_metadata() which returns every setting
    available on this workspace, including those migrated from workspace-conf.
    """
    records: list[dict] = []

    try:
        metadata_list = list(
            ws_client.workspace_settings_v2.list_workspace_settings_metadata(
                page_size=1000
            )
        )
    except Exception as e:
        logger.error(
            "Cannot list workspace settings metadata for %s: %s",
            workspace.workspace_name,
            e,
        )
        return records

    logger.info(
        "Discovered %d workspace setting definitions for %s",
        len(metadata_list),
        workspace.workspace_name,
    )

    for meta in metadata_list:
        name = meta.name
        value = _safe_get_workspace_setting(ws_client, name)

        records.append(
            _build_record(
                collected_at=collected_at,
                account_id=account_id,
                scope="workspace",
                workspace_id=workspace.workspace_id,
                workspace_name=workspace.workspace_name,
                setting_name=name,
                setting_value=value,
                setting_type=getattr(meta, "type", "unknown"),
                source="settings_v2",
                category=None,
                preview_phase=_get_preview_phase(meta),
                description=getattr(meta, "description", None),
            )
        )

    return records


def _safe_get_account_setting(client: AccountClient, name: str) -> str:
    """Retrieve account setting value with graceful error handling."""
    try:
        setting = client.settings_v2.get_public_account_setting(name=name)
        return _extract_value(setting)
    except Exception as e:
        logger.debug("Could not get account setting '%s': %s", name, e)
        return "<unavailable>"


def _safe_get_workspace_setting(client: WorkspaceClient, name: str) -> str:
    """Retrieve workspace setting value with graceful error handling."""
    try:
        setting = client.workspace_settings_v2.get_public_workspace_setting(
            name=name
        )
        return _extract_value(setting)
    except Exception as e:
        logger.debug("Could not get workspace setting '%s': %s", name, e)
        return "<unavailable>"


def _extract_value(setting) -> str:
    """Extract a string value from a Setting object.

    The Settings V2 API returns a Setting object with effective_* fields
    that contain the actual resolved value. We check effective fields first,
    then fall back to direct value fields.
    """
    if setting is None:
        return "<null>"

    # The Setting object has typed effective_* fields -- check these first
    for attr in (
        "effective_boolean_val",
        "effective_string_val",
        "effective_integer_val",
    ):
        val = getattr(setting, attr, None)
        if val is not None:
            inner = getattr(val, "value", None)
            if inner is not None:
                return str(inner)

    # Check all effective_* fields for complex message types
    for attr_name in dir(setting):
        if not attr_name.startswith("effective_"):
            continue
        val = getattr(setting, attr_name, None)
        if val is None:
            continue
        # Use as_dict() if available to get a clean representation
        if hasattr(val, "as_dict") and callable(val.as_dict):
            d = val.as_dict()
            if d:
                # For single-field dicts, return just the value
                values = [v for v in d.values() if v is not None]
                if len(values) == 1:
                    return str(values[0])
                if values:
                    return str(d)
        # Fallback: look for non-None non-callable attributes
        for inner_attr in dir(val):
            if inner_attr.startswith("_") or callable(getattr(val, inner_attr, None)):
                continue
            inner_val = getattr(val, inner_attr, None)
            if inner_val is not None:
                return str(inner_val)

    # Direct value fields (older patterns)
    for attr in ("boolean_val", "string_val", "integer_val", "value"):
        val = getattr(setting, attr, None)
        if val is not None:
            inner = getattr(val, "value", val)
            if inner is not None:
                return str(inner)

    # Last resort: try the name field to confirm we got a Setting object
    if hasattr(setting, "name"):
        return "<not-set>"

    return str(setting)


def _get_preview_phase(meta) -> Optional[str]:
    """Extract preview_phase from metadata, if present."""
    phase = getattr(meta, "preview_phase", None)
    if phase is None:
        return None
    return str(phase)


def _build_record(
    collected_at: datetime,
    account_id: str,
    scope: str,
    workspace_id: Optional[int],
    workspace_name: Optional[str],
    setting_name: str,
    setting_value: str,
    setting_type: str,
    source: str,
    category: str,
    preview_phase: Optional[str],
    description: Optional[str],
) -> dict:
    """Build a flat dictionary record for a collected setting."""
    return {
        "collected_at": collected_at.isoformat(),
        "account_id": account_id,
        "scope": scope,
        "workspace_id": workspace_id,
        "workspace_name": workspace_name,
        "setting_name": setting_name,
        "setting_value": setting_value,
        "setting_type": str(setting_type),
        "source": source,
        "category": category,
        "preview_phase": preview_phase,
        "description": description,
    }
