"""Dataset factories for the metadata-driven Debezium CDC pipeline.

Uses ``pyspark.pipelines`` — the current Lakeflow Declarative Pipelines
Python API (the ``dlt`` module is its legacy alias). Every factory takes
a :class:`~cdc_framework.config.TableConfig` and registers the datasets
for that one table, so the pipeline scales to any number of source
tables by looping over the metadata.

Per table the medallion layout is::

    <name>_bronze       raw Debezium events (Auto Loader, streaming)
    <name>_silver_feed  flattened, quality-gated CDC feed (temp view)
    <name>_silver       merged current state / history (AUTO CDC flow)

plus any number of metadata-defined gold materialized views.

Schema evolution: bronze uses Auto Loader inference with
``schemaEvolutionMode=addNewColumns`` (unparsed data is rescued, never
lost), the silver feed selects the row image dynamically from the
current schema, and the AUTO CDC flow evolves the silver table's schema
automatically when new columns appear.
"""

from __future__ import annotations

from pyspark import pipelines as dp
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from .config import GoldViewConfig, TableConfig
from .debezium import (
    CDC_METADATA_COLUMNS,
    CDC_OP,
    CDC_POS,
    CDC_SOURCE_DB,
    CDC_SOURCE_TABLE,
    CDC_TS_MS,
    DELETE_PREDICATE,
    RESCUE_EXPECTATION,
    TOMBSTONE_EXPECTATION,
    key_expectation,
    sanitize_column_name,
)


def register_table(spark: SparkSession, table: TableConfig) -> None:
    """Register the bronze/silver datasets for one source table."""
    _register_bronze(spark, table)
    _register_silver_feed(spark, table)
    _register_silver(table)


def register_gold_view(spark: SparkSession, view: GoldViewConfig) -> None:
    """Register one metadata-defined gold materialized view."""

    def gold() -> DataFrame:
        return spark.sql(view.sql)

    dp.materialized_view(
        name=view.name,
        comment=view.comment or "Gold view defined in pipeline metadata.",
        table_properties={"quality": "gold"},
    )(gold)


def _register_bronze(spark: SparkSession, table: TableConfig) -> None:
    """Ingest raw Debezium JSON events with Auto Loader.

    The schema is inferred and evolves (``addNewColumns``): when a new
    column appears upstream the update restarts once and picks it up,
    while values that do not fit the current schema land in
    ``_rescued_data`` instead of being dropped.
    """

    def bronze() -> DataFrame:
        reader = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        )
        if table.schema_hints:
            reader = reader.option(
                "cloudFiles.schemaHints", table.schema_hints
            )
        return reader.load(table.path)

    bronze = dp.expect(*RESCUE_EXPECTATION)(bronze)
    dp.table(
        name=table.bronze_name,
        comment=(
            f"Raw Debezium CDC events for "
            f"{table.source_database}.{table.name}, ingested with "
            f"Auto Loader from {table.path}"
        ),
        table_properties={"quality": "bronze"},
    )(bronze)


def _register_silver_feed(spark: SparkSession, table: TableConfig) -> None:
    """Flatten the Debezium envelope into a quality-gated CDC feed.

    Deletes carry the row image in ``before``, everything else in
    ``after``. Bookkeeping columns get a ``_cdc_`` prefix so they never
    collide with source columns, and metadata-defined expectations and
    column transforms are applied here, before the merge.
    """

    def silver_feed() -> DataFrame:
        events = spark.readStream.table(table.bronze_name).select(
            "payload.*"
        )
        events = events.withColumn(
            "_row_image", _row_image_column(events.schema)
        )
        row_type = events.schema["_row_image"].dataType
        df = events.select(
            _metadata_columns(events.schema)
            + _row_columns(row_type, table.exclude_columns)
        )
        for column, expression in table.column_transforms.items():
            df = df.withColumn(column, F.expr(expression))
        return df

    silver_feed = dp.expect_or_drop(*TOMBSTONE_EXPECTATION)(silver_feed)
    drop_rules = {
        **dict([key_expectation(table.keys)]),
        **table.expectations.get("drop", {}),
    }
    silver_feed = dp.expect_all_or_drop(drop_rules)(silver_feed)
    if table.expectations.get("warn"):
        silver_feed = dp.expect_all(table.expectations["warn"])(silver_feed)
    if table.expectations.get("fail"):
        silver_feed = dp.expect_all_or_fail(table.expectations["fail"])(
            silver_feed
        )
    dp.temporary_view(
        name=table.silver_feed_name,
        comment=f"Flattened CDC feed for {table.name} (drives AUTO CDC).",
    )(silver_feed)


def _register_silver(table: TableConfig) -> None:
    """Merge the CDC feed into the silver table with an AUTO CDC flow.

    ``ts_ms`` alone has millisecond granularity and can tie for rapid
    changes to the same row, so the binlog position breaks ties
    deterministically.
    """
    dp.create_streaming_table(
        name=table.silver_name,
        comment=table.comment
        or (
            f"{table.name} rows merged from Debezium CDC "
            f"(SCD type {table.scd_type}, deletes applied)."
        ),
        table_properties={"quality": "silver"},
        cluster_by=list(table.cluster_by) or None,
    )
    dp.create_auto_cdc_flow(
        target=table.silver_name,
        source=table.silver_feed_name,
        keys=list(table.keys),
        sequence_by=F.struct(F.col(CDC_TS_MS), F.col(CDC_POS)),
        apply_as_deletes=F.expr(DELETE_PREDICATE),
        except_column_list=list(CDC_METADATA_COLUMNS),
        stored_as_scd_type=table.scd_type,
    )


def _row_image_column(schema: StructType) -> Column:
    """Pick ``before`` for deletes and ``after`` otherwise.

    If ``before`` was never populated in the data seen so far, Auto
    Loader infers it as a non-struct null; fall back to ``after`` alone
    rather than failing the update on a struct/null type mismatch.
    """
    names = set(schema.fieldNames())
    before_is_struct = "before" in names and isinstance(
        schema["before"].dataType, StructType
    )
    if not before_is_struct:
        return F.col("after")
    return (
        F.when(F.col("op") == "d", F.col("before")).otherwise(F.col("after"))
    )


def _metadata_columns(schema: StructType) -> list[Column]:
    """Build the ``_cdc_*`` bookkeeping columns from the envelope.

    ``source.pos`` (the MySQL binlog position) is the sequencing
    tiebreaker; if a connector does not provide it, a null column keeps
    the feed schema stable and sequencing falls back to ``ts_ms``.
    """
    pos = F.lit(None).cast("long")
    if "source" in schema.fieldNames():
        source_type = schema["source"].dataType
        if (
            isinstance(source_type, StructType)
            and "pos" in source_type.fieldNames()
        ):
            pos = F.col("source.pos")
    return [
        F.col("op").alias(CDC_OP),
        F.col("ts_ms").alias(CDC_TS_MS),
        pos.alias(CDC_POS),
        F.col("source.db").alias(CDC_SOURCE_DB),
        F.col("source.table").alias(CDC_SOURCE_TABLE),
    ]


def _row_columns(
    row_type: StructType, exclude: tuple[str, ...]
) -> list[Column]:
    """Select the row-image fields with Delta-safe column names."""
    excluded = set(exclude)
    columns = []
    for field in row_type.fields:
        alias = sanitize_column_name(field.name)
        if field.name in excluded or alias in excluded:
            continue
        columns.append(F.col(f"_row_image.`{field.name}`").alias(alias))
    return columns
