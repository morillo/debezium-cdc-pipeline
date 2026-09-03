"""Metadata-driven Debezium CDC framework for Lakeflow Declarative Pipelines.

Public surface:
    - :mod:`cdc_framework.config` — metadata model and loader (Spark-free).
    - :mod:`cdc_framework.debezium` — envelope constants and helpers.
    - :mod:`cdc_framework.flows` — dataset factories (requires a pipeline
      runtime; import it only inside a pipeline update).
"""

from cdc_framework.config import (
    ConfigError,
    GoldViewConfig,
    PipelineConfig,
    TableConfig,
    load_config,
)

__all__ = [
    "ConfigError",
    "GoldViewConfig",
    "PipelineConfig",
    "TableConfig",
    "load_config",
]
