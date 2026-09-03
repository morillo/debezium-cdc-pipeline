"""Unit tests for the Debezium helpers (no Spark required)."""

from cdc_framework.debezium import (
    CDC_METADATA_COLUMNS,
    CDC_OP,
    CDC_POS,
    CDC_TS_MS,
    DELETE_PREDICATE,
    TOMBSTONE_EXPECTATION,
    key_expectation,
    sanitize_column_name,
)


def test_sanitize_replaces_delta_invalid_characters():
    assert sanitize_column_name("first name") == "first_name"
    assert sanitize_column_name("a,b;c{d}e(f)g=h") == "a_b_c_d_e_f_g_h"
    assert sanitize_column_name("tab\tand\nnewline") == "tab_and_newline"


def test_sanitize_keeps_valid_names_unchanged():
    assert sanitize_column_name("order_id") == "order_id"


def test_key_expectation_covers_every_key():
    name, constraint = key_expectation(("id", "region"))
    assert name == "valid_primary_key"
    assert constraint == "`id` IS NOT NULL AND `region` IS NOT NULL"


def test_cdc_bookkeeping_contract():
    # The sequencing and delete predicates must reference columns that
    # are part of the bookkeeping set stripped from the silver table.
    assert CDC_TS_MS in CDC_METADATA_COLUMNS
    assert CDC_POS in CDC_METADATA_COLUMNS
    assert CDC_OP in DELETE_PREDICATE
    assert CDC_OP in TOMBSTONE_EXPECTATION[1]
