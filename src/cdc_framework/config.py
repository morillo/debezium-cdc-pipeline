"""Configuration model and loader for the Debezium CDC pipeline.

The pipeline is metadata driven: every source table is described by an
entry in a YAML (or JSON) file, and the pipeline builds one bronze
table, one silver feed view, and one silver streaming table per entry,
plus any configured gold materialized views. The path of the metadata
file and the SDLC environment name are supplied through the pipeline
``configuration`` block (``cdc.config_path`` / ``cdc.environment``), so
the same code runs unchanged in dev, test, and prod.

Everything in this module is plain Python (no Spark imports) so it can
be unit tested anywhere, including CI runners without a Spark runtime.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

EXPECTATION_ACTIONS = ("warn", "drop", "fail")
VALID_SCD_TYPES = (1, 2)
DEFAULT_TOPIC_TEMPLATE = "mysql.{database}.{table}"

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class ConfigError(ValueError):
    """Raised when the pipeline metadata file is invalid.

    The message aggregates every problem found so a bad file can be
    fixed in one pass instead of one failed pipeline update at a time.
    """


@dataclass(frozen=True)
class TableConfig:
    """Everything the pipeline needs to know about one source table.

    Attributes:
        name: Logical table name; used to derive dataset names.
        source_database: Source MySQL database (Debezium topic segment).
        path: Fully resolved landing directory for this table's events.
        keys: Primary-key column names used by the AUTO CDC merge.
        scd_type: 1 (current state) or 2 (full history) for silver.
        comment: Optional human description propagated to the catalog.
        schema_hints: Optional Auto Loader ``cloudFiles.schemaHints``
            string, e.g. ``"payload.after.total DECIMAL(18,2)"``.
        column_transforms: Column name -> SQL expression applied in the
            silver feed (e.g. Debezium epoch ints to real timestamps).
        exclude_columns: Row columns to drop before the silver merge.
        cluster_by: Liquid clustering columns for the silver table.
        expectations: Data-quality rules per action ("warn", "drop",
            "fail"), each a mapping of rule name -> SQL constraint.
    """

    name: str
    source_database: str
    path: str
    keys: tuple[str, ...]
    scd_type: int = 1
    comment: str = ""
    schema_hints: str = ""
    column_transforms: dict[str, str] = field(default_factory=dict)
    exclude_columns: tuple[str, ...] = ()
    cluster_by: tuple[str, ...] = ()
    expectations: dict[str, dict[str, str]] = field(default_factory=dict)

    @property
    def bronze_name(self) -> str:
        """Name of the raw-events streaming table."""
        return f"{self.name}_bronze"

    @property
    def silver_feed_name(self) -> str:
        """Name of the flattened view feeding the AUTO CDC flow."""
        return f"{self.name}_silver_feed"

    @property
    def silver_name(self) -> str:
        """Name of the merged silver streaming table."""
        return f"{self.name}_silver"


@dataclass(frozen=True)
class GoldViewConfig:
    """A gold-layer materialized view defined entirely in metadata."""

    name: str
    sql: str
    comment: str = ""


@dataclass(frozen=True)
class PipelineConfig:
    """The fully validated pipeline metadata for one environment."""

    environment: str
    landing_root: str
    tables: tuple[TableConfig, ...]
    gold_views: tuple[GoldViewConfig, ...] = ()


def load_config(path: str | Path, environment: str) -> PipelineConfig:
    """Load and validate the metadata file for the given environment.

    ``{env}`` placeholders in ``landing_root`` (and per-table ``path``
    overrides) are replaced with ``environment``, which is how one
    metadata file serves dev, test, and prod.

    Raises:
        ConfigError: if the file is missing, unparsable, or invalid.
    """
    raw = _parse_file(Path(path))
    errors: list[str] = []

    landing_root = _string(raw, "landing_root", errors)
    landing_root = landing_root.replace("{env}", environment).rstrip("/")
    topic_template = raw.get("topic_template", DEFAULT_TOPIC_TEMPLATE)
    defaults = raw.get("defaults") or {}
    if not isinstance(defaults, dict):
        errors.append("'defaults' must be a mapping.")
        defaults = {}

    tables = _build_tables(
        raw.get("tables"), defaults, landing_root, topic_template,
        environment, errors,
    )
    gold_views = _build_gold_views(raw.get("gold"), errors)

    _check_unique_names(tables, gold_views, errors)
    if errors:
        raise ConfigError(
            "Invalid pipeline metadata in {}:\n- {}".format(
                path, "\n- ".join(errors)
            )
        )
    return PipelineConfig(
        environment=environment,
        landing_root=landing_root,
        tables=tables,
        gold_views=gold_views,
    )


def _parse_file(path: Path) -> dict[str, Any]:
    """Parse a YAML or JSON metadata file into a dictionary."""
    if not path.is_file():
        raise ConfigError(f"Pipeline metadata file not found: {path}")
    text = path.read_text(encoding="utf-8")
    try:
        if path.suffix.lower() in (".yaml", ".yml"):
            import yaml  # deferred: only needed for YAML files

            parsed = yaml.safe_load(text)
        else:
            parsed = json.loads(text)
    except Exception as exc:  # noqa: BLE001 - rewrap with file context
        raise ConfigError(f"Could not parse {path}: {exc}") from exc
    if not isinstance(parsed, dict):
        raise ConfigError(f"{path} must contain a mapping at the top level.")
    return parsed


def _build_tables(
    entries: Any,
    defaults: dict[str, Any],
    landing_root: str,
    topic_template: str,
    environment: str,
    errors: list[str],
) -> tuple[TableConfig, ...]:
    if not isinstance(entries, list) or not entries:
        errors.append("'tables' must be a non-empty list.")
        return ()
    tables = []
    for index, entry in enumerate(entries):
        if not isinstance(entry, dict):
            errors.append(f"tables[{index}] must be a mapping.")
            continue
        merged = {**defaults, **entry}
        table = _build_table(
            merged, index, landing_root, topic_template, environment, errors
        )
        if table is not None:
            tables.append(table)
    return tuple(tables)


def _build_table(
    entry: dict[str, Any],
    index: int,
    landing_root: str,
    topic_template: str,
    environment: str,
    errors: list[str],
) -> TableConfig | None:
    label = f"tables[{index}]"
    name = entry.get("name", "")
    if not _IDENTIFIER.match(str(name)):
        errors.append(f"{label}: 'name' must be a valid identifier.")
        return None
    label = f"table '{name}'"

    database = str(entry.get("source_database", ""))
    if not database:
        errors.append(f"{label}: 'source_database' is required.")

    keys = _string_tuple(entry.get("keys"))
    if not keys:
        errors.append(f"{label}: 'keys' must list at least one column.")

    scd_type = entry.get("scd_type", 1)
    if scd_type not in VALID_SCD_TYPES:
        errors.append(f"{label}: 'scd_type' must be one of {VALID_SCD_TYPES}.")

    expectations = _expectations(entry.get("expectations"), label, errors)

    path = str(
        entry.get("path")
        or "{}/{}".format(
            landing_root,
            topic_template.format(database=database, table=name),
        )
    ).replace("{env}", environment)

    return TableConfig(
        name=str(name),
        source_database=database,
        path=path,
        keys=keys,
        scd_type=int(scd_type) if scd_type in VALID_SCD_TYPES else 1,
        comment=str(entry.get("comment", "")),
        schema_hints=str(entry.get("schema_hints", "")),
        column_transforms=_string_mapping(entry.get("column_transforms")),
        exclude_columns=_string_tuple(entry.get("exclude_columns")),
        cluster_by=_string_tuple(entry.get("cluster_by")),
        expectations=expectations,
    )


def _build_gold_views(
    entries: Any, errors: list[str]
) -> tuple[GoldViewConfig, ...]:
    if entries is None:
        return ()
    if not isinstance(entries, list):
        errors.append("'gold' must be a list.")
        return ()
    views = []
    for index, entry in enumerate(entries):
        label = f"gold[{index}]"
        if not isinstance(entry, dict):
            errors.append(f"{label} must be a mapping.")
            continue
        name = entry.get("name", "")
        sql = str(entry.get("sql", "")).strip()
        if not _IDENTIFIER.match(str(name)):
            errors.append(f"{label}: 'name' must be a valid identifier.")
            continue
        if not sql:
            errors.append(f"gold view '{name}': 'sql' is required.")
            continue
        views.append(
            GoldViewConfig(
                name=str(name), sql=sql, comment=str(entry.get("comment", ""))
            )
        )
    return tuple(views)


def _expectations(
    value: Any, label: str, errors: list[str]
) -> dict[str, dict[str, str]]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        errors.append(f"{label}: 'expectations' must be a mapping.")
        return {}
    result: dict[str, dict[str, str]] = {}
    for action, rules in value.items():
        if action not in EXPECTATION_ACTIONS:
            errors.append(
                f"{label}: unknown expectation action '{action}' "
                f"(expected one of {EXPECTATION_ACTIONS})."
            )
            continue
        if not isinstance(rules, dict):
            errors.append(f"{label}: expectations.{action} must be a mapping.")
            continue
        result[action] = {str(k): str(v) for k, v in rules.items()}
    return result


def _check_unique_names(
    tables: tuple[TableConfig, ...],
    gold_views: tuple[GoldViewConfig, ...],
    errors: list[str],
) -> None:
    seen: set[str] = set()
    for name in [t.name for t in tables] + [g.name for g in gold_views]:
        if name in seen:
            errors.append(f"Duplicate dataset name '{name}'.")
        seen.add(name)


def _string(raw: dict[str, Any], key: str, errors: list[str]) -> str:
    value = raw.get(key)
    if not isinstance(value, str) or not value.strip():
        errors.append(f"'{key}' is required and must be a non-empty string.")
        return ""
    return value.strip()


def _string_tuple(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        value = value.split(",")
    return tuple(str(v).strip() for v in value if str(v).strip())


def _string_mapping(value: Any) -> dict[str, str]:
    if not isinstance(value, dict):
        return {}
    return {str(k): str(v) for k, v in value.items()}
