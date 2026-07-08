# Databricks Configuration Insights

Discover, track, and monitor **every** account and workspace setting — including
**preview features** — across your Databricks estate, with zero hardcoded setting
lists and platform-native drift detection.

The tool packages everything as a **Databricks Asset Bundle (DAB)**: a scheduled
collection **job**, an AI/BI **dashboard**, three SQL **alerts**, and a Lakehouse
**monitor**. Deploy it straight from a Databricks Git folder, or with the
Databricks CLI from your laptop.

---

## Overview

| Capability | How it works |
|---|---|
| **Complete discovery** | The Settings V2 metadata API (`list_*_settings_metadata()`) is self-describing, so **every** available setting is enumerated automatically — nothing is hardcoded. |
| **Preview feature tracking** | Settings carry a `preview_phase` (`PRIVATE_PREVIEW`, `BETA`, `PUBLIC_PREVIEW`, …). Previews are surfaced automatically with a dedicated enabled/disabled heatmap. |
| **Schema evolution** | The Delta table is written with `mergeSchema=true`, so new metadata fields added by Databricks appear as new columns with no DDL changes. |
| **Platform-native drift** | A Lakehouse Monitoring **TimeSeries** profile computes drift metrics daily — no handcrafted comparison SQL. |
| **Alerting** | Three SQL alerts fire on config drift, cross-workspace security inconsistencies, and newly enabled preview features. |
| **Zero maintenance** | New settings/previews added by Databricks are captured on the next run; deprecated ones simply stop appearing. |

---

## Screenshots

> Screenshots are captured from a live deployment. To regenerate them from your
> own workspace, open the deployed **Configuration Insights** dashboard and export
> each page (see [`docs/images/README.md`](docs/images/README.md)).

### Account Overview
Totals, category/scope breakdowns, and collection history.

![Account Overview](docs/images/01-account-overview.png)

### Preview Features Heatmap
Which previews are **enabled** vs **disabled**, by workspace and preview phase.

![Preview Features Heatmap](docs/images/02-preview-heatmap.png)

### Configuration Drift
Changes per day (from Lakehouse Monitoring) cross-filtering a change-detail table.

![Configuration Drift](docs/images/03-configuration-drift.png)

### Security Posture
Security-relevant settings and cross-workspace inconsistencies.

![Security Posture](docs/images/04-security-posture.png)

---

## Architecture

```
┌───────────────────────────────────────────────────────────────────────────┐
│  Settings V2 Metadata API (self-describing)                                │
│    Account:   list_account_settings_metadata()  / get_public_account_*     │
│    Workspace: list_workspace_settings_metadata() / get_public_workspace_*  │
└───────────────────────────────────┬───────────────────────────────────────┘
                                     │  collector job (daily, serverless)
                                     │  append with mergeSchema=true
                                     ▼
┌───────────────────────────────────────────────────────────────────────────┐
│  Delta: <catalog>.<schema>.settings_history   (CDF on, schema-evolving)    │
│    collected_at │ scope │ workspace │ setting_name │ setting_value │ …      │
│  Views: settings_latest · workspace_comparison · preview_heatmap           │
└───────────────────────────────────┬───────────────────────────────────────┘
                                     │  Lakehouse Monitoring (TimeSeries)
                                     ▼
┌───────────────────────────────────────────────────────────────────────────┐
│  Auto-generated:  settings_history_profile_metrics                         │
│                   settings_history_drift_metrics                           │
└───────────────────────────────────┬───────────────────────────────────────┘
                                     ▼
┌───────────────────────────────────────────────────────────────────────────┐
│  AI/BI Dashboard (4 pages)      +      3 SQL Alerts                         │
│    Overview · Preview Heatmap          drift · security · new-preview       │
│    Config Drift · Security                                                  │
└───────────────────────────────────────────────────────────────────────────┘
```

---

## Repository structure

```
databricks-config-insights/
├── databricks.yml                      # DAB root: variables, targets, wheel auto-build
├── pyproject.toml                      # Python package (built into a wheel automatically)
├── README.md
├── resources/                          # DAB resources, split by type
│   ├── jobs/
│   │   └── collector.job.yml           # Scheduled collection job (serverless)
│   ├── dashboards/
│   │   ├── config_insights.dashboard.yml   # AI/BI dashboard resource
│   │   └── config_insights.lvdash.json     # Dashboard definition (4 pages)
│   └── alerts/
│       └── config_alerts.yml           # 3 SQL alerts (drift, security, new preview)
├── sql/
│   └── create_tables.sql               # Reference DDL (tables are created by the job)
├── src/
│   └── config_insights/                # Collector Python package
│       ├── __main__.py                 # Job entry point (`collect`)
│       ├── collector.py                # Orchestration
│       ├── discovery.py                # Dynamic Settings V2 discovery (no hardcoded keys)
│       ├── writer.py                   # Schema-evolving Delta writer + views
│       └── monitoring.py               # Lakehouse Monitoring setup
└── docs/
    └── images/                         # Dashboard screenshots
```

---

## Prerequisites

| Requirement | Detail |
|---|---|
| Unity Catalog | For the Delta tables, views, and Lakehouse Monitoring. |
| SQL warehouse | Serverless or Pro — used by the dashboard and alerts. |
| Workspace admin | The job identity needs to read workspace settings. |
| Account admin *(optional)* | Only required for cross-workspace scanning via an account ID. Without it the tool runs in **workspace-only** mode. |
| Databricks CLI ≥ 0.281.0 | For DAB deploy (`dataset_catalog` support on dashboards). |
| Python ≥ 3.10 | To build the collector wheel (done automatically on deploy). |

---

## Quick start

The collector wheel is built **automatically** during `databricks bundle deploy`
(see the `artifacts` block in `databricks.yml`) — you do not need to run
`python -m build` yourself.

### Option A — Deploy from a Databricks Git folder

1. In your workspace, go to **Workspaces → your user → Add → Git folder** and
   clone this repository.
2. Open a **Web Terminal** in the Git folder (or use a notebook with `%sh`).
3. Deploy and run:

   ```bash
   databricks bundle deploy -t dev \
     --var="warehouse_id=<your-warehouse-id>" \
     --var="catalog=<your-catalog>"

   databricks bundle run config_collector -t dev \
     --var="warehouse_id=<your-warehouse-id>" \
     --var="catalog=<your-catalog>"
   ```

### Option B — Deploy from your laptop with the CLI

```bash
git clone <this-repo-url>
cd databricks-config-insights

# Authenticate once (creates/uses a CLI profile)
databricks auth login --host https://<your-workspace>.cloud.databricks.com

# Deploy (builds the wheel, uploads files, creates job + dashboard + alerts)
databricks bundle deploy -t dev -p <profile> \
  --var="warehouse_id=<your-warehouse-id>" \
  --var="catalog=<your-catalog>"

# Kick off the first collection (also creates the Lakehouse monitor)
databricks bundle run config_collector -t dev -p <profile> \
  --var="warehouse_id=<your-warehouse-id>" \
  --var="catalog=<your-catalog>"
```

### After the first run

- Open the **Configuration Insights** dashboard in your workspace.
- The Lakehouse **monitor** is created on `settings_history`; its drift metrics
  populate a few minutes after the first two collection runs exist.
- The three SQL **alerts** evaluate daily and email the deploying user.

---

## Configuration

All values are DAB **variables** — override them per deploy with `--var`, or set
defaults in `databricks.yml`.

| Variable | Default | Description |
|---|---|---|
| `catalog` | `main` | Unity Catalog for the settings tables, views, and monitor output. |
| `schema` | `config_insights` | Schema within the catalog. |
| `warehouse_id` | *(required)* | SQL warehouse for the dashboard and alerts. |
| `account_id` | `""` (empty) | Optional account ID for cross-workspace scanning (requires account admin). Empty ⇒ workspace-only mode. |

### Scheduling

Defaults are staggered so each stage runs after the previous one finishes:

| Stage | Cron (UTC) |
|---|---|
| Collection job | `0 0 6 * * ?` (06:00) |
| Monitor refresh | triggered after every job run (+ `0 30 6 * * ?` fallback) |
| SQL alerts | `0 0 7 * * ?` (07:00) |

Adjust the cron expressions in `resources/jobs/collector.job.yml` and
`resources/alerts/config_alerts.yml`.

---

## How it works

### Dynamic discovery (no hardcoded keys)

The Settings V2 metadata endpoints return every setting's name, type,
description, docs link, and `preview_phase`. The collector iterates that metadata
and reads each value — so new settings and previews are picked up automatically:

```python
for meta in ws_client.settings_v2.list_workspace_settings_metadata():
    value = ws_client.settings_v2.get_public_workspace_setting(name=meta.name)
    # …normalized into a row: setting_name, setting_value, category, preview_phase, …
```

### Schema evolution

```python
df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(table)
```

If Databricks adds new metadata fields later (e.g. `deprecated_date`), they become
new columns automatically — no migration required.

### Lakehouse Monitoring instead of custom drift SQL

```python
ws_client.quality_monitors.create(
    table_name=f"{catalog}.{schema}.settings_history",
    time_series=MonitorTimeSeries(timestamp_col="collected_at", granularities=["1 day"]),
    slicing_exprs=["scope", "category", "workspace_name"],
)
```

This yields statistical drift metrics (per day, sliced by scope/category/workspace)
that feed both the dashboard's Configuration Drift page and the drift SQL alert.

---

## Multi-workspace (account-level) scanning

Set `account_id` **and** run the job with an identity that is an **account admin**
(typically a service principal). The collector then enumerates all workspaces via
`AccountClient.workspaces.list()` and scans each one, enabling the cross-workspace
comparison and security-inconsistency views.

If the job identity is only a workspace admin (the default), the account API
returns `Not Found` and the tool automatically falls back to **workspace-only**
mode — still fully functional for the current workspace.

---

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `Falling back to workspace-only mode` | The job identity is not an account admin. Expected unless you configured account-level scanning. |
| Dashboard widgets show *no data* | Run the collector at least once; drift needs **two** runs before metrics appear. |
| `Metastore storage root URL does not exist` on `CREATE CATALOG` | Point `catalog` at an existing catalog — the job uses `USE CATALOG` + `CREATE SCHEMA`, it does not create catalogs. |
| Alert query errors on `*_drift_metrics` | The monitor's metric tables are created on first monitor refresh; let one collection cycle complete. |
