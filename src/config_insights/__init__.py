"""Databricks Configuration Insights Tool.

Dynamically discovers and collects all account and workspace settings
using the Settings V2 metadata API, classifies each into a functional
category via ai_classify, and writes results to a schema-evolving Delta
table; drift is detected in SQL by the dashboard and alerts.
"""

__version__ = "0.5.0"
