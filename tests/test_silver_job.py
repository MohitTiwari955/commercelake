import json
from pathlib import Path

import pandas as pd

from commercelake.bronze import SOURCE_COLUMNS
from commercelake.ingest import ingest_file
from commercelake.silver_job import build_silver

HEADER = ",".join(SOURCE_COLUMNS)
FULL = "2020-04-24 11:50:39 UTC,100,555,777,electronics.tablet,samsung,162.01,999"
SHORT = "2020-05-02 11:12:20 UTC,101,556,,0.02,1515915625453675584"
PLACEHOLDER = "1970-01-01 00:33:40 UTC,102,557,777,electronics.tablet,samsung,10.00,999"
TOO_OLD = "2019-12-31 23:59:59 UTC,103,558,777,electronics.tablet,samsung,10.00,999"


def make_bronze(tmp_path, lines):
    source = tmp_path / "tiny.csv"
    source.write_text("\n".join([HEADER] + lines) + "\n", encoding="utf-8")
    summary = ingest_file(source, tmp_path / "bronze")
    return Path(summary["folder"])


def test_silver_job_counts_and_flags(tmp_path):
    bronze = make_bronze(tmp_path, [FULL, SHORT, FULL, PLACEHOLDER, TOO_OLD])
    summary = build_silver(bronze, tmp_path / "silver")
    assert summary["bronze_rows"] == 5
    assert summary["silver_rows"] == 4
    assert summary["quarantine_rows"] == 1
    assert summary["flags"]["was_realigned"] == 1
    assert summary["flags"]["event_time_is_placeholder"] == 1
    assert summary["flags"]["is_exact_repeat"] == 1


def test_quarantined_row_keeps_its_reason(tmp_path):
    bronze = make_bronze(tmp_path, [FULL, TOO_OLD])
    summary = build_silver(bronze, tmp_path / "silver")
    part = next(Path(summary["folder"]).glob("quarantine/part-*.parquet"))
    quarantine = pd.read_parquet(part)
    assert list(quarantine["quarantine_reason"]) == ["event_time_unusable"]
    assert list(quarantine["order_id"]) == ["103"]


def test_silver_is_not_built_twice(tmp_path):
    bronze = make_bronze(tmp_path, [FULL, SHORT])
    first = build_silver(bronze, tmp_path / "silver")
    second = build_silver(bronze, tmp_path / "silver")
    assert first["status"] == "built"
    assert second["status"] == "skipped"


def test_summary_file_is_written(tmp_path):
    bronze = make_bronze(tmp_path, [FULL, SHORT])
    summary = build_silver(bronze, tmp_path / "silver")
    saved = json.loads((Path(summary["folder"]) / "_summary.json").read_text(encoding="utf-8"))
    assert saved["silver_rows"] == 2