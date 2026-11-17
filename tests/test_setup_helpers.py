"""Unit tests for the helpers in Notebooks/00_setup.py, on a local Spark session (see conftest.py)."""
from datetime import datetime

from pyspark.sql import functions as F


def test_normalize_street_removes_quotes_prefix_and_extra_spaces(spark, setup):
    df = spark.createDataFrame(
        [(" רח' של״ג ",), ("רחוב  דיזנגוף",), ("המלך ג'ורג'",), ("קרן היסוד",)], "street string"
    )
    keys = [r[0] for r in df.select(setup["normalize_street"](F.col("street"))).collect()]
    assert keys == ["שלג", "דיזנגוף", "המלך גורג", "קרן היסוד"]


def test_daily_compensation_is_area_times_100_capped_at_10000(spark, setup):
    df = spark.createDataFrame([(40,), (100,), (150,), (0,)], "area int")
    comp = [r[0] for r in df.select(setup["daily_compensation"](F.col("area"))).collect()]
    assert comp == [4000, 10000, 10000, 0]


def test_latest_per_key_keeps_the_newest_snapshot_per_key(spark, setup):
    df = spark.createDataFrame(
        [
            ("a", 1, datetime(2026, 1, 1)),
            ("a", 2, datetime(2026, 1, 2)),   # newest for key a
            ("b", 3, datetime(2026, 1, 1)),
        ],
        "id string, value int, ingested_at timestamp",
    )
    rows = {r["id"]: r["value"] for r in setup["latest_per_key"](df, ["id"]).collect()}
    assert rows == {"a": 2, "b": 3}


def test_changed_condition_compares_only_data_columns(spark, setup):
    df = spark.createDataFrame([], "id string, name string, area int, run_id string, bronze_ingested_at timestamp")
    condition = setup["changed_condition"](df, ["id"])
    assert condition == "NOT (t.`name` <=> s.`name`) OR NOT (t.`area` <=> s.`area`)"
    assert setup["key_condition"](["id", "side"]) == "t.`id` = s.`id` AND t.`side` = s.`side`"


def test_upsert_rows_inserts_new_updates_changed_and_skips_identical(spark, setup):
    table = "test_upsert"
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    first = spark.createDataFrame([("a", 10), ("b", 20)], "id string, area int")
    assert setup["upsert_rows"](first, table, ["id"]) == (2, 0)          # first run: table created

    second = spark.createDataFrame([("a", 10), ("b", 25), ("c", 30)], "id string, area int")
    assert setup["upsert_rows"](second, table, ["id"]) == (1, 1)         # c inserted, b updated, a unchanged

    result = {r["id"]: r["area"] for r in spark.table(table).collect()}
    assert result == {"a": 10, "b": 25, "c": 30}
