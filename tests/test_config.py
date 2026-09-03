"""Unit tests for the metadata model and loader (no Spark required)."""

import pytest

from cdc_framework.config import ConfigError, load_config

VALID_YAML = """
landing_root: /Volumes/cdc_{env}/landing/debezium
topic_template: mysql.{database}.{table}

defaults:
  source_database: inventory
  keys: [id]

tables:
  - name: customers
    scd_type: 2
    cluster_by: [id]
    column_transforms:
      created_at: timestamp_millis(created_at)
    expectations:
      drop:
        valid_email: email IS NULL OR email LIKE '%@%'
  - name: orders
    keys: [order_id]
    path: /Volumes/other_{env}/landing/special/orders

gold:
  - name: gold_totals
    comment: Totals.
    sql: SELECT 1 AS one
"""


def write(tmp_path, text, suffix=".yaml"):
    path = tmp_path / f"tables{suffix}"
    path.write_text(text, encoding="utf-8")
    return path


def test_loads_valid_yaml_and_resolves_environment(tmp_path):
    config = load_config(write(tmp_path, VALID_YAML), environment="test")

    assert config.environment == "test"
    assert config.landing_root == "/Volumes/cdc_test/landing/debezium"

    customers, orders = config.tables
    assert customers.name == "customers"
    assert customers.keys == ("id",)  # from defaults
    assert customers.scd_type == 2
    assert customers.path == (
        "/Volumes/cdc_test/landing/debezium/mysql.inventory.customers"
    )
    assert customers.bronze_name == "customers_bronze"
    assert customers.silver_feed_name == "customers_silver_feed"
    assert customers.silver_name == "customers_silver"
    assert customers.expectations["drop"] == {
        "valid_email": "email IS NULL OR email LIKE '%@%'"
    }

    # Per-table overrides win over defaults and derived paths.
    assert orders.keys == ("order_id",)
    assert orders.path == "/Volumes/other_test/landing/special/orders"

    (gold,) = config.gold_views
    assert gold.name == "gold_totals"
    assert gold.sql == "SELECT 1 AS one"


def test_loads_json_config(tmp_path):
    text = (
        '{"landing_root": "/mnt/landing", "tables": '
        '[{"name": "t1", "source_database": "db", "keys": ["id"]}]}'
    )
    config = load_config(write(tmp_path, text, ".json"), environment="dev")
    assert config.tables[0].path == "/mnt/landing/mysql.db.t1"


def test_missing_file_raises(tmp_path):
    with pytest.raises(ConfigError, match="not found"):
        load_config(tmp_path / "absent.yaml", environment="dev")


def test_invalid_yaml_raises(tmp_path):
    with pytest.raises(ConfigError, match="Could not parse"):
        load_config(write(tmp_path, "a: [unclosed"), environment="dev")


def test_all_errors_reported_at_once(tmp_path):
    text = """
tables:
  - name: "bad name!"
  - name: t1
    keys: []
    scd_type: 3
    expectations:
      explode: {r: "1 = 1"}
  - name: t1
    source_database: db
    keys: [id]
gold:
  - name: g1
"""
    with pytest.raises(ConfigError) as excinfo:
        load_config(write(tmp_path, text), environment="dev")
    message = str(excinfo.value)
    assert "'landing_root' is required" in message
    assert "'name' must be a valid identifier" in message
    assert "'keys' must list at least one column" in message
    assert "'scd_type' must be one of" in message
    assert "unknown expectation action 'explode'" in message
    assert "Duplicate dataset name 't1'" in message
    assert "gold view 'g1': 'sql' is required" in message


def test_empty_tables_rejected(tmp_path):
    with pytest.raises(ConfigError, match="non-empty list"):
        load_config(
            write(tmp_path, "landing_root: /mnt/x\ntables: []"),
            environment="dev",
        )


def test_keys_accept_comma_separated_string(tmp_path):
    text = """
landing_root: /mnt/landing
tables:
  - name: t1
    source_database: db
    keys: "id, region"
"""
    config = load_config(write(tmp_path, text), environment="dev")
    assert config.tables[0].keys == ("id", "region")
