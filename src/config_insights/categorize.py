"""AI-based setting categorization via the ``ai_classify`` SQL function.

Each setting is classified into exactly one of a configurable set of
functional categories (e.g. governance, ingestion, AI, ML, compute, …).
Categories are a *functional* dimension only -- the preview/lifecycle
dimension is tracked separately via ``preview_phase`` and the Preview
Features section of the dashboard, so "preview" is deliberately NOT a
category label.

Classification is cached in a ``setting_category_map`` Delta table keyed by
setting name and a hash of the active label set. A setting is (re)classified
only when it is new or when the configured label list changes, so a normal
run makes zero ``ai_classify`` calls once the map is warm.

There is intentionally no keyword-heuristic fallback: categorization relies
solely on ``ai_classify``. If the call fails (e.g. the workspace lacks
serverless / Foundation Model access) the affected settings are left
uncategorized (``category = NULL``) and a clear error is logged.
"""

import hashlib
import logging

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

MAP_TABLE = "setting_category_map"


def _labels_version(labels: list[str]) -> str:
    """Stable short hash of the (order-insensitive) label set."""
    joined = "|".join(sorted(label.strip() for label in labels))
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()[:12]


def _labels_array_sql(labels: list[str]) -> str:
    """Render labels as a SQL ``array('a', 'b', ...)`` literal (SQL-escaped)."""
    escaped = ", ".join("'" + label.strip().replace("'", "''") + "'" for label in labels)
    return f"array({escaped})"


def classify_settings(
    spark: SparkSession,
    records: list[dict],
    catalog: str,
    schema: str,
    labels: list[str],
) -> None:
    """Populate ``record['category']`` for every record, in place.

    Uses a cached map table so only new/unseen settings (or a changed label
    set) are sent to ``ai_classify``. On classification failure the category
    is left as ``None`` (no fallback).
    """
    if not records:
        return
    if not labels:
        logger.warning("No categories configured; leaving settings uncategorized")
        for r in records:
            r["category"] = None
        return

    map_table = f"{catalog}.{schema}.{MAP_TABLE}"
    lv = _labels_version(labels)

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {map_table} (
            setting_name STRING,
            category STRING,
            labels_version STRING,
            classified_at TIMESTAMP
        ) USING delta
        """
    )

    # Distinct settings (name -> best description) discovered this run.
    distinct: dict[str, str] = {}
    for r in records:
        name = r["setting_name"]
        if name not in distinct or not distinct[name]:
            distinct[name] = r.get("description") or ""

    # Names already classified under the *current* label set.
    already = {
        row["setting_name"]
        for row in spark.sql(
            f"SELECT setting_name FROM {map_table} WHERE labels_version = '{lv}'"
        ).collect()
    }
    to_classify = {n: d for n, d in distinct.items() if n not in already}

    if to_classify:
        logger.info(
            "Classifying %d new/changed settings via ai_classify (%d cached)",
            len(to_classify),
            len(already),
        )
        try:
            rows = [(n, d) for n, d in to_classify.items()]
            tmp = spark.createDataFrame(rows, "setting_name STRING, descr STRING")
            tmp.createOrReplaceTempView("_settings_to_classify")

            spark.sql(
                f"""
                MERGE INTO {map_table} t
                USING (
                    SELECT
                        setting_name,
                        ai_classify(
                            concat(setting_name, ': ', coalesce(descr, '')),
                            {_labels_array_sql(labels)}
                        ) AS category,
                        '{lv}' AS labels_version,
                        current_timestamp() AS classified_at
                    FROM _settings_to_classify
                ) s
                ON t.setting_name = s.setting_name AND t.labels_version = s.labels_version
                WHEN MATCHED THEN UPDATE SET
                    t.category = s.category, t.classified_at = s.classified_at
                WHEN NOT MATCHED THEN INSERT *
                """
            )
        except Exception as e:
            logger.error(
                "ai_classify categorization failed (%s). Settings from this run "
                "will be left uncategorized (category = NULL).",
                e,
            )

    # Load the map for the current label set and apply to records.
    cat_map = {
        row["setting_name"]: row["category"]
        for row in spark.sql(
            f"SELECT setting_name, category FROM {map_table} WHERE labels_version = '{lv}'"
        ).collect()
    }
    for r in records:
        r["category"] = cat_map.get(r["setting_name"])

    classified = sum(1 for r in records if r["category"])
    logger.info(
        "Categorization complete: %d/%d settings categorized",
        classified,
        len(records),
    )
