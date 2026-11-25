"""Tests for the Spark Bronze job, using tiny CSV files."""
import pytest

from commercelake.bronze import SOURCE_COLUMNS
from commercelake.spark_bronze import build_bronze_spark

HEADER = ",".join(SOURCE_COLUMNS)
FULL = "2020-04-24 11:50:39 UTC,100,555,777,electronics.tablet,samsung,162.01,999"
SHORT = "2020-05-02 11:12:20 UTC,101,556,,0.02,1515915625453675584"


def make_csv(tmp_path, lines, header=HEADER):
    source = tmp_path / "tiny.csv"
    source.write_text("\n".join([header] + lines) + "\n", encoding="utf-8")
    return source


def read_back(spark, summary):
    return spark.read.parquet(summary["folder"]).orderBy("_source_row_number").collect()


def test_row_numbers_and_field_counts(spark, tmp_path):
    source = make_csv(tmp_path, [FULL, SHORT, FULL])
    summary = build_bronze_spark(spark, source, tmp_path / "bronze")
    rows = read_back(spark, summary)
    assert summary["status"] == "loaded"
    assert summary["rows"] == 3
    assert [r["_source_row_number"] for r in rows] == [1, 2, 3]
    assert [r["_field_count"] for r in rows] == [8, 6, 8]


def test_short_line_is_kept_raw_not_fixed(spark, tmp_path):
    source = make_csv(tmp_path, [SHORT])
    summary = build_bronze_spark(spark, source, tmp_path / "bronze")
    row = read_back(spark, summary)[0]
    assert row["category_id"] == ""
    assert row["category_code"] == "0.02"
    assert row["brand"] == "1515915625453675584"
    assert row["price"] is None
    assert row["user_id"] is None


def test_same_file_is_not_loaded_twice(spark, tmp_path):
    source = make_csv(tmp_path, [FULL, SHORT])
    first = build_bronze_spark(spark, source, tmp_path / "bronze")
    second = build_bronze_spark(spark, source, tmp_path / "bronze")
    assert first["status"] == "loaded"
    assert second["status"] == "skipped"


def test_wrong_header_is_rejected(spark, tmp_path):
    source = make_csv(tmp_path, [FULL], header="a,b,c")
    with pytest.raises(ValueError):
        build_bronze_spark(spark, source, tmp_path / "bronze")