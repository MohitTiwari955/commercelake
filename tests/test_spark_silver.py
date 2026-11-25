"""Tests for Spark Silver: cleaning rules, repeat flag and the Silver job."""
import json
from decimal import Decimal
from pathlib import Path

from pyspark.sql import functions as F

from commercelake.bronze import SOURCE_COLUMNS
from commercelake.spark_bronze import build_bronze_spark
from commercelake.spark_silver import add_exact_repeat_flag, clean_bronze_spark
from commercelake.spark_silver_job import build_silver_spark

HEADER = ",".join(SOURCE_COLUMNS)
FULL = "2020-04-24 11:50:39 UTC,100,555,777,electronics.tablet,samsung,162.01,999"
OTHER = "2020-04-24 11:50:39 UTC,101,555,777,electronics.tablet,samsung,162.01,999"
SHORT = "2020-05-02 11:12:20 UTC,101,556,,0.02,1515915625453675584"
ODD_SHORT = "2020-05-02 11:12:20 UTC,101,556,,not-a-price,1515915625453675584"
PLACEHOLDER = "1970-01-01 00:33:40 UTC,102,557,777,electronics.tablet,samsung,10.00,999"
TOO_OLD = "2019-12-31 23:59:59 UTC,103,558,777,electronics.tablet,samsung,10.00,999"
ZERO = "2020-04-24 11:50:39 UTC,104,559,777,electronics.tablet,samsung,0,999"
NEGATIVE = "2020-04-24 11:50:39 UTC,105,560,777,electronics.tablet,samsung,-5,999"
NO_ORDER = "2020-04-24 11:50:39 UTC,,555,777,electronics.tablet,samsung,162.01,999"


def bronze_load(spark, tmp_path, lines):
    """Write a tiny CSV and load it into a Spark Bronze folder."""
    source = tmp_path / "tiny.csv"
    source.write_text("\n".join([HEADER] + lines) + "\n", encoding="utf-8")
    summary = build_bronze_spark(spark, source, tmp_path / "bronze")
    return Path(summary["folder"])


def clean(spark, tmp_path, lines):
    folder = bronze_load(spark, tmp_path, lines)
    bronze = spark.read.parquet(str(folder / "part-*.parquet"))
    return clean_bronze_spark(bronze)


def test_full_row_is_typed_and_kept(spark, tmp_path):
    silver, quarantine = clean(spark, tmp_path, [FULL])
    row = silver.withColumn("t", F.date_format("event_time", "yyyy-MM-dd HH:mm:ss")).collect()[0]
    assert quarantine.count() == 0
    assert row["price"] == Decimal("162.01")
    assert row["t"] == "2020-04-24 11:50:39"
    assert not row["was_realigned"]


def test_short_line_is_realigned(spark, tmp_path):
    silver, quarantine = clean(spark, tmp_path, [SHORT])
    row = silver.collect()[0]
    assert quarantine.count() == 0
    assert row["price"] == Decimal("0.02")
    assert row["user_id"] == "1515915625453675584"
    assert row["category_code"] is None
    assert row["brand"] is None
    assert row["was_realigned"]
    assert row["is_uncatalogued_item"]


def test_placeholder_time_is_flagged_not_dropped(spark, tmp_path):
    silver, quarantine = clean(spark, tmp_path, [PLACEHOLDER])
    row = silver.collect()[0]
    assert quarantine.count() == 0
    assert row["event_time"] is None
    assert row["event_time_is_placeholder"]


def test_impossible_time_goes_to_quarantine(spark, tmp_path):
    silver, quarantine = clean(spark, tmp_path, [TOO_OLD])
    assert silver.count() == 0
    assert quarantine.collect()[0]["quarantine_reason"] == "event_time_unusable"


def test_short_line_with_text_in_price_slot_goes_to_quarantine(spark, tmp_path):
    silver, quarantine = clean(spark, tmp_path, [ODD_SHORT])
    assert silver.count() == 0
    assert quarantine.collect()[0]["quarantine_reason"] == "short_line_unexpected_layout"


def test_zero_price_is_flagged_and_negative_price_is_quarantined(spark, tmp_path):
    silver, quarantine = clean(spark, tmp_path, [ZERO, NEGATIVE])
    assert silver.count() == 1
    assert silver.collect()[0]["price_is_zero"]
    assert quarantine.collect()[0]["quarantine_reason"] == "negative_price"


def test_every_bronze_row_ends_up_in_exactly_one_table(spark, tmp_path):
    lines = [FULL, SHORT, PLACEHOLDER, TOO_OLD, NO_ORDER]
    silver, quarantine = clean(spark, tmp_path, lines)
    assert silver.count() + quarantine.count() == len(lines)
    assert quarantine.count() == 2


def test_exact_repeat_is_found_across_partitions(spark, tmp_path):
    silver, _ = clean(spark, tmp_path, [FULL, OTHER, FULL, FULL])
    flagged = add_exact_repeat_flag(silver.repartition(4))
    rows = flagged.orderBy("_source_row_number").collect()
    assert [r["is_exact_repeat"] for r in rows] == [False, False, True, True]


def test_job_counts_and_flags(spark, tmp_path):
    load = bronze_load(spark, tmp_path, [FULL, SHORT, FULL, PLACEHOLDER, TOO_OLD])
    summary = build_silver_spark(spark, load, tmp_path / "silver")
    assert summary["bronze_rows"] == 5
    assert summary["silver_rows"] == 4
    assert summary["quarantine_rows"] == 1
    assert summary["flags"]["was_realigned"] == 1
    assert summary["flags"]["event_time_is_placeholder"] == 1
    assert summary["flags"]["is_exact_repeat"] == 1


def test_quarantined_row_keeps_its_reason_and_raw_values(spark, tmp_path):
    load = bronze_load(spark, tmp_path, [FULL, TOO_OLD])
    summary = build_silver_spark(spark, load, tmp_path / "silver")
    rows = spark.read.parquet(summary["folder"] + "/quarantine").collect()
    assert [r["quarantine_reason"] for r in rows] == ["event_time_unusable"]
    assert [r["order_id"] for r in rows] == ["103"]


def test_silver_is_not_built_twice(spark, tmp_path):
    load = bronze_load(spark, tmp_path, [FULL, SHORT])
    first = build_silver_spark(spark, load, tmp_path / "silver")
    second = build_silver_spark(spark, load, tmp_path / "silver")
    assert first["status"] == "built"
    assert second["status"] == "skipped"


def test_summary_file_is_written(spark, tmp_path):
    load = bronze_load(spark, tmp_path, [FULL, SHORT])
    summary = build_silver_spark(spark, load, tmp_path / "silver")
    saved = json.loads((Path(summary["folder"]) / "_summary.json").read_text(encoding="utf-8"))
    assert saved["silver_rows"] == 2