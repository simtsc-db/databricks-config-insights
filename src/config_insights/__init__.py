"""Databricks Configuration Insights Tool.

Dynamically discovers and collects all account and workspace settings
using the Settings V2 metadata API. Writes results to a schema-evolving
Delta table; drift is detected in SQL by the dashboard and alerts.
"""

__version__ = "0.4.0"
