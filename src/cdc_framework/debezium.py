"""Debezium-specific constants and naming helpers.

Kept free of Spark imports so it is unit testable without a runtime.

CDC bookkeeping columns carried through the silver feed use a ``_cdc_``
prefix so they can never collide with real source columns, and they are
stripped from the silver table by the AUTO CDC ``except_column_list``.
"""

from __future__ import annotations

CDC_OP = "_cdc_op"
CDC_TS_MS = "_cdc_ts_ms"
CDC_POS = "_cdc_pos"
CDC_SOURCE_DB = "_cdc_source_db"
CDC_SOURCE_TABLE = "_cdc_source_table"

#: All bookkeeping columns, i.e. the AUTO CDC ``except_column_list``.
CDC_METADATA_COLUMNS = (
    CDC_OP,
    CDC_TS_MS,
    CDC_POS,
    CDC_SOURCE_DB,
    CDC_SOURCE_TABLE,
)

#: Debezium delete marker, evaluated against the silver feed.
DELETE_PREDICATE = f"{CDC_OP} = 'd'"

#: Kafka tombstones / malformed events carry no ``op``; drop them.
TOMBSTONE_EXPECTATION = ("valid_debezium_op", f"{CDC_OP} IS NOT NULL")

#: Warn (and count in event log metrics) when Auto Loader rescued data.
RESCUE_EXPECTATION = ("no_rescued_data", "_rescued_data IS NULL")

#: Characters Delta Lake does not accept in column names.
_INVALID_COLUMN_CHARS = set(" ,;{}()\n\t=")


def sanitize_column_name(name: str) -> str:
    """Replace characters Delta rejects in column names with ``_``."""
    return "".join(
        "_" if char in _INVALID_COLUMN_CHARS else char for char in name
    )


def key_expectation(keys: tuple[str, ...]) -> tuple[str, str]:
    """Build a drop expectation asserting every merge key is present.

    Rows without a full primary key cannot be merged deterministically
    (e.g. truncated events), so they are dropped and surfaced in the
    pipeline event log rather than corrupting the silver table.
    """
    constraint = " AND ".join(f"`{key}` IS NOT NULL" for key in keys)
    return ("valid_primary_key", constraint)
