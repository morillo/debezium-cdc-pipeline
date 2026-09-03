"""Entry point for the metadata-driven Debezium CDC pipeline.

This is the single source file registered with the Lakeflow pipeline.
It reads two settings from the pipeline ``configuration`` block:

    cdc.environment  SDLC environment name (dev | test | prod); used to
                     resolve ``{env}`` placeholders in landing paths.
    cdc.config_path  Path to the table metadata file (YAML or JSON),
                     e.g. a workspace file deployed by the bundle or a
                     Unity Catalog volume path.

It then registers bronze/silver datasets for every table in the
metadata, plus any gold materialized views. Adding a new source table
is a metadata change only — no code changes required.
"""

import sys
from pathlib import Path

# The bundle deploys src/ as the pipeline root; make cdc_framework
# importable regardless of how the runtime seeds sys.path.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from pyspark.sql import SparkSession  # noqa: E402

from cdc_framework import flows, load_config  # noqa: E402

spark = SparkSession.getActiveSession()


def _required_setting(key: str) -> str:
    value = spark.conf.get(key, None)
    if not value:
        raise ValueError(
            f"Pipeline configuration '{key}' must be set in the pipeline "
            "settings (see resources/cdc_pipeline.pipeline.yml)."
        )
    return value


config = load_config(
    path=_required_setting("cdc.config_path"),
    environment=_required_setting("cdc.environment"),
)

for table in config.tables:
    flows.register_table(spark, table)
for view in config.gold_views:
    flows.register_gold_view(spark, view)
