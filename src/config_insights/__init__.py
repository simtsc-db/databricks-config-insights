"""Databricks Configuration Insights Tool.

Dynamically discovers and collects all account and workspace settings
using the Settings V2 metadata API. Writes results to a schema-evolving
Delta table monitored by Lakehouse Monitoring for drift detection.
"""

__version__ = "0.3.0"
