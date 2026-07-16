# Databricks Configuration Insights

Discover, track, and monitor **every** account and workspace setting — including
**preview features** — across your Databricks estate, with zero hardcoded setting
lists and exact, per-setting drift detection.

The tool packages everything as a **Databricks Asset Bundle (DAB)**: a scheduled
collection **job**, an AI/BI **dashboard**, and two SQL **alerts**. Deploy it
straight from a Databricks Git folder, or with the Databricks CLI from your
laptop.

---

## Overview

| Capability | How it works |
|---|---|
| **Complete discovery** | The Settings V2 metadata API (`list_*_settings_metadata()`) is self-describing, so **every** available setting is enumerated automatically — nothing is hardcoded. |
| **AI categorization** | Each setting is classified into a **configurable** functional category (governance, ingestion, AI, ML, compute, …) with `ai_classify`. Results are cached, so only new settings are ever re-classified. |
| **Preview feature tracking** | Settings carry a `preview_phase` (`PRIVATE_PREVIEW`, `BETA`, `PUBLIC_PREVIEW`, …). Previews are surfaced automatically with a dedicated enabled/disabled section. Preview is a *lifecycle* dimension, kept separate from the functional category. |
| **Schema evolution** | The Delta table is written with `mergeSchema=true`, so new metadata fields added by Databricks appear as new columns with no DDL changes. |
| **Change detection** | Exact snapshot-to-snapshot comparison classifies every change as **value_changed**, **added**, or **removed** — so settings that appear or disappear from the Settings V2 API are caught too, not just value flips. Pure SQL over `settings_history` powers both the Configuration Drift page and the alerts. |
| **Alerting** | Two SQL alerts fire on config drift and newly enabled preview features, and list **exactly what changed** in the notification body. |
| **Zero maintenance** | New settings/previews added by Databricks are captured on the next run; deprecated ones simply stop appearing. |

---

## Screenshots

> Screenshots are captured from a live deployment. To regenerate them from your
> own workspace, open the deployed **Configuration Insights** dashboard and export
> each page (see [`docs/images/README.md`](docs/images/README.md)).

### Overview & Previews
Totals, category/scope breakdowns, collection history, and the preview-feature
section (status/phase bars + enabled/disabled table). A **Workspace** filter
scopes the whole page, and widgets that share a dataset **cross-filter** each
other (e.g. click a category to filter the counters and scope pie, or a preview
status/phase bar to filter the preview table).

![Overview & Previews](docs/images/01-overview.png)

### Configuration Drift
A **Workspace** filter plus changes-per-day bars that cross-filter a
change-detail table (value_changed / added / removed), plus cross-workspace
consistency.

![Configuration Drift](docs/images/02-configuration-drift.png)

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
│  Delta: <catalog>.<schema>.settings_history   (schema-evolving)            │
│    collected_at │ scope │ workspace │ setting_name │ setting_value │ …      │
│  Views: settings_latest · workspace_comparison · preview_heatmap           │
└───────────────────────────────────┬───────────────────────────────────────┘
                                     │  exact snapshot-to-snapshot SQL
                                     │  (value_changed / added / removed)
                                     ▼
┌───────────────────────────────────────────────────────────────────────────┐
│  AI/BI Dashboard (2 pages)      +      2 SQL Alerts                         │
│    Overview & Previews                 drift · new-preview                  │
│    Config Drift                                                             │
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
│   │   └── config_insights.lvdash.json     # Dashboard definition (2 pages)
│   └── alerts/
│       └── config_alerts.yml           # 2 SQL alerts (drift, new preview)
├── sql/
│   └── create_tables.sql               # Reference DDL (tables are created by the job)
├── src/
│   └── config_insights/                # Collector Python package
│       ├── __main__.py                 # Job entry point (`collect`)
│       ├── collector.py                # Orchestration
│       ├── discovery.py                # Dynamic Settings V2 discovery (no hardcoded keys)
│       ├── categorize.py               # ai_classify categorization + cached map table
│       └── writer.py                   # Schema-evolving Delta writer + views
└── docs/
    └── images/                         # Dashboard screenshots
```

---

## Prerequisites

| Requirement | Detail |
|---|---|
| Unity Catalog | For the Delta tables and views. |
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

# Kick off the first collection (creates the tables and views)
databricks bundle run config_collector -t dev -p <profile> \
  --var="warehouse_id=<your-warehouse-id>" \
  --var="catalog=<your-catalog>"
```

### After the first run

- Open the **Configuration Insights** dashboard in your workspace.
- Drift (value_changed / added / removed) appears once **two** collection runs
  exist, since each setting is compared to its previous snapshot.
- The two SQL **alerts** evaluate daily and email the deploying user.

---

## Configuration

All values are DAB **variables** — override them per deploy with `--var`, or set
defaults in `databricks.yml`.

| Variable | Default | Description |
|---|---|---|
| `catalog` | `main` | Unity Catalog for the settings tables and views. **Must be an existing catalog you can access** — override it per deploy (see troubleshooting for the `main` gotcha). |
| `schema` | `config_insights` | Schema within the catalog. |
| `warehouse_id` | *(required)* | SQL warehouse for the dashboard and alerts. |
| `categories` | `governance,ingestion,AI,ML,compute,marketplace,platform,other` | Comma-separated functional categories used by `ai_classify`. Preview is *not* a category (it's tracked via `preview_phase`). Changing this list re-classifies settings on the next run. |
| `account_id` | `"none"` | Optional account ID for cross-workspace scanning (requires account admin). `none` (or empty) ⇒ workspace-only mode. Must be a non-empty string — Terraform rejects null job parameters. |

> **Note on `catalog`:** nothing is hardcoded — the catalog is always the
> `catalog` variable. The default `main` is just a convention; if your workspace
> has no accessible `main` catalog, pass `--var="catalog=<your-catalog>"` on
> every `deploy`/`run` (or set `BUNDLE_VAR_catalog` in your environment).

**Single, harmonized storage location.** Everything the tool creates — the
`settings_history` table, the `settings_latest` / `workspace_comparison` /
`preview_heatmap` views, the dashboard datasets, and the SQL alerts — lives in
the same **`${catalog}.${schema}`**. Change those two variables and the whole
tool (job, dashboard, alerts) follows. Nothing is pinned to a hardcoded catalog
or schema.

### Scheduling

Defaults are staggered so each stage runs after the previous one finishes:

| Stage | Cron (UTC) |
|---|---|
| Collection job | `0 0 6 * * ?` (06:00) |
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

### AI categorization (`ai_classify`)

Each setting is classified into exactly one **functional** category from the
configurable `categories` list using the `ai_classify` SQL function, based on
the setting name and description:

```sql
ai_classify(concat(setting_name, ': ', description),
            array('governance','ingestion','AI','ML','compute','marketplace','platform','other'))
```

Results are cached in a `setting_category_map` table keyed by setting name and a
hash of the active label set, so a warm run makes **zero** `ai_classify` calls —
only genuinely new settings (or a changed `categories` list) are re-classified.
Categorization relies solely on `ai_classify` (no keyword fallback); this
requires the workspace to have serverless / Foundation Model APIs available. The
**preview/lifecycle** dimension is kept separate (`preview_phase`), so "preview"
is deliberately not a category.

### Dashboard filtering

- **Workspace filter** — each page has a single-select *Workspace* filter that
  scopes every widget sourced from a workspace-aware dataset. With account-level
  scanning off there is a single workspace to choose; it becomes a real
  comparison control once multiple workspaces are collected.
- **Cross-filtering** — clicking a data point filters the other widgets that
  share the same dataset **on the same page** (e.g. click a category bar to
  filter the counters and the scope pie; click a preview status/phase bar to
  filter the preview table). Cross-filtering cannot span pages or datasets — a
  Lakeview platform constraint.

### Schema evolution

```python
df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(table)
```

If Databricks adds new metadata fields later (e.g. `deprecated_date`), they become
new columns automatically — no migration required.

### Change detection: exact per-setting comparison

Drift shown on the dashboard and evaluated by the alerts is an **exact
snapshot comparison**. Each setting is compared against its previous
observation and every change is classified as one of:

- **value_changed** — the setting exists in both runs but its value differs,
- **added** — the setting is present now but was absent in the prior run,
- **removed** — the setting was present before but the Settings V2 API no
  longer returns it (e.g. deprecated/renamed).

To catch **added**/**removed** (not just value flips), the query builds a grid
of *(setting × run)* and left-joins it to the actual observations, so a missing
observation is a real signal rather than an invisible gap. It needs no
run-timestamp alignment, and it lets the alert report exactly which settings
changed and how.

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
| Dashboard widgets show `[INSUFFICIENT_PERMISSIONS] Catalog 'main' is not accessible` | You deployed without overriding `catalog`, so it defaulted to `main`. Redeploy with `--var="catalog=<your-catalog>"` (the same one the collector wrote to). |
| Categories all `NULL` / *Settings by Category* empty | `ai_classify` was unavailable (needs serverless + Foundation Model APIs). Check the job logs for the categorization error; there is no keyword fallback by design. |
| Alerts always report 0 / never trigger | Drift and new-preview detection compares each setting to its **previous** observation, so at least **two** collection runs must exist before anything can fire. |
