# Databricks Configuration Insights

Discover, track, and monitor **every** account and workspace setting — including
**preview features** — across your Databricks estate, with zero hardcoded setting
lists and platform-native drift detection.

The tool packages everything as a **Databricks Asset Bundle (DAB)**: a scheduled
collection **job**, an AI/BI **dashboard**, two SQL **alerts**, and a Lakehouse
**monitor**. Deploy it straight from a Databricks Git folder, or with the
Databricks CLI from your laptop.

---

## Overview

| Capability | How it works |
|---|---|
| **Complete discovery** | The Settings V2 metadata API (`list_*_settings_metadata()`) is self-describing, so **every** available setting is enumerated automatically — nothing is hardcoded. |
| **Preview feature tracking** | Settings carry a `preview_phase` (`PRIVATE_PREVIEW`, `BETA`, `PUBLIC_PREVIEW`, …). Previews are surfaced automatically with a dedicated enabled/disabled heatmap. |
| **Schema evolution** | The Delta table is written with `mergeSchema=true`, so new metadata fields added by Databricks appear as new columns with no DDL changes. |
| **Change detection** | An exact value-change comparison — a `LAG()` window over `settings_history` — powers the Configuration Drift dashboard page and the alerts. A Lakehouse Monitoring **TimeSeries** profile runs in parallel for native profiling/drift in the Databricks UI. |
| **Alerting** | Two SQL alerts fire on config drift and newly enabled preview features, and list **exactly what changed** in the notification body. |
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
Changes per day cross-filtering a change-detail table, plus cross-workspace consistency.

![Configuration Drift](docs/images/03-configuration-drift.png)

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
└───────────────┬───────────────────────────────────────┬───────────────────┘
                │ direct LAG() value comparison          │ Lakehouse Monitoring
                ▼                                         ▼   (TimeSeries, parallel)
┌───────────────────────────────────────────┐   ┌───────────────────────────┐
│  AI/BI Dashboard (3 pages) + 2 SQL Alerts  │   │ settings_history_*_metrics │
│    Overview · Preview Heatmap              │   │ (native profiling / drift  │
│    Config Drift          drift · new-prev  │   │  dashboards in the UI)     │
└───────────────────────────────────────────┘   └───────────────────────────┘
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
│   │   └── config_insights.lvdash.json     # Dashboard definition (3 pages)
│   └── alerts/
│       └── config_alerts.yml           # 2 SQL alerts (drift, new preview)
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
- The two SQL **alerts** evaluate daily and email the deploying user.

---

## Configuration

All values are DAB **variables** — override them per deploy with `--var`, or set
defaults in `databricks.yml`.

| Variable | Default | Description |
|---|---|---|
| `catalog` | `main` | Unity Catalog for the settings tables, views, and monitor output. |
| `schema` | `config_insights` | Schema within the catalog. |
| `warehouse_id` | *(required)* | SQL warehouse for the dashboard and alerts. |
| `account_id` | `"none"` | Optional account ID for cross-workspace scanning (requires account admin). `none` (or empty) ⇒ workspace-only mode. Must be a non-empty string — Terraform rejects null job parameters. |

**Single, harmonized storage location.** Everything the tool creates — the
`settings_history` table, the `settings_latest` / `workspace_comparison` /
`preview_heatmap` views, the dashboard datasets, the SQL alerts, **and** the
Lakehouse Monitoring metric tables (`settings_history_profile_metrics`,
`settings_history_drift_metrics`) — lives in the same **`${catalog}.${schema}`**.
Change those two variables and the whole tool (job, dashboard, alerts, monitor)
follows. Nothing is pinned to a hardcoded catalog or schema.

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

### Change detection: exact value comparison + native monitoring

Drift shown on the dashboard and evaluated by the alerts is an **exact
value-change** comparison — a `LAG()` window over `settings_history` compares
each setting to its own previous observation. This needs no statistical drift
metrics and no run-timestamp alignment, and it lets the alert report exactly
which settings flipped:

```sql
LAG(setting_value) OVER (
  PARTITION BY setting_name, COALESCE(workspace_id, 0) ORDER BY collected_at)
```

In parallel, a Lakehouse Monitoring **TimeSeries** profile is created on the same
table for native, out-of-the-box profiling and drift dashboards in the Databricks
UI (sliced by scope/category/workspace):

```python
ws_client.quality_monitors.create(
    table_name=f"{catalog}.{schema}.settings_history",
    time_series=MonitorTimeSeries(timestamp_col="collected_at", granularities=["1 day"]),
    slicing_exprs=["scope", "category", "workspace_name"],
)
```

---

## Multi-workspace (account-level) scanning

Set `account_id` **and** run the job with an identity that is an **account admin**
(typically a service principal). The collector then enumerates all workspaces via
`AccountClient.workspaces.list()` and scans each one, enabling the cross-workspace
comparison view (Configuration Drift page).

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
| Alerts always report 0 / never trigger | Drift and new-preview detection compares each setting to its **previous** observation, so at least **two** collection runs must exist before anything can fire. |
