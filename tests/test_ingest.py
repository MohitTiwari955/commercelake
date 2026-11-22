import pytest
import pyarrow.parquet as pq
from pathlib import Path

from commercelake.bronze import SOURCE_COLUMNS
from commercelake.ingest import ingest_file

HEADER = ",".join(SOURCE_COLUMNS)
FULL = "2020-04-24 11:50:39 UTC,100,555,777,electronics.tablet,samsung,162.01,999"
SHORT = "2020-05-02 11:12:20 UTC,101,556,,0.02,888"


def make_source(tmp_path, lines):
    path = tmp_path / "tiny.csv"
    path.write_text("\n".join([HEADER] + lines) + "\n", encoding="utf-8")
    return path


def read_rows(load_folder):
    rows = []
    for part in sorted(Path(load_folder).glob("part-*.parquet")):
        rows.extend(pq.read_table(str(part)).to_pylist())
    return rows


def test_every_source_row_lands_in_bronze(tmp_path):
    source = make_source(tmp_path, [FULL, SHORT, FULL, SHORT, FULL])
    summary = ingest_file(source, tmp_path / "bronze", batch_size=2)
    rows = read_rows(summary["folder"])
    assert summary["rows"] == 5
    assert summary["parts"] == 3
    assert [r["_source_row_number"] for r in rows] == [1, 2, 3, 4, 5]


def test_short_line_survives_unchanged(tmp_path):
    source = make_source(tmp_path, [FULL, SHORT])
    summary = ingest_file(source, tmp_path / "bronze")
    rows = read_rows(summary["folder"])
    assert rows[1]["_field_count"] == 6
    assert rows[1]["category_code"] == "0.02"
    assert rows[1]["price"] is None


def test_same_file_is_not_loaded_twice(tmp_path):
    source = make_source(tmp_path, [FULL, SHORT])
    first = ingest_file(source, tmp_path / "bronze")
    second = ingest_file(source, tmp_path / "bronze")
    assert first["status"] == "loaded"
    assert second["status"] == "skipped"
    assert len(list((tmp_path / "bronze").glob("sha256-*"))) == 1


def test_failed_load_leaves_no_partial_bronze(tmp_path):
    bad_line = "a,b,c,d,e,f,g,h,i"
    source = make_source(tmp_path, [FULL, bad_line])
    with pytest.raises(ValueError):
        ingest_file(source, tmp_path / "bronze")
    assert list((tmp_path / "bronze").glob("sha256-*")) == []