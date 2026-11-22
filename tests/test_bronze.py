from datetime import datetime, timezone

import pytest

from commercelake.bronze import SOURCE_COLUMNS, make_bronze_columns, read_batches

HEADER = ",".join(SOURCE_COLUMNS)
FULL = "2020-04-24 11:50:39 UTC,100,555,777,electronics.tablet,samsung,162.01,999"
SHORT = "2020-05-02 11:12:20 UTC,101,556,,0.02,888"


def write_csv(tmp_path, lines):
    path = tmp_path / "tiny.csv"
    path.write_text("\n".join([HEADER] + lines) + "\n", encoding="utf-8")
    return path


def lineage_args():
    return dict(
        source_file="tiny.csv",
        sha256="abc",
        batch_id="b1",
        ingested_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )


def test_batches_cover_every_row_once(tmp_path):
    path = write_csv(tmp_path, [FULL, SHORT, FULL, SHORT, FULL])
    batches = list(read_batches(path, batch_size=2))
    assert [first for first, rows in batches] == [1, 3, 5]
    assert sum(len(rows) for first, rows in batches) == 5


def test_short_line_is_kept_short_not_fixed(tmp_path):
    path = write_csv(tmp_path, [FULL, SHORT])
    first, rows = next(read_batches(path))
    columns = make_bronze_columns(first, rows, **lineage_args())
    assert columns["_field_count"] == [8, 6]
    assert columns["category_code"] == ["electronics.tablet", "0.02"]
    assert columns["price"] == ["162.01", None]
    assert columns["category_id"] == ["777", ""]


def test_row_numbers_follow_the_file(tmp_path):
    path = write_csv(tmp_path, [FULL, SHORT, FULL])
    first, rows = next(read_batches(path))
    columns = make_bronze_columns(first, rows, **lineage_args())
    assert columns["_source_row_number"] == [1, 2, 3]


def test_wrong_header_is_rejected(tmp_path):
    path = tmp_path / "bad.csv"
    path.write_text("a,b,c\n1,2,3\n", encoding="utf-8")
    with pytest.raises(ValueError):
        list(read_batches(path))


def test_line_with_too_many_fields_is_rejected():
    rows = [["x"] * 9]
    with pytest.raises(ValueError):
        make_bronze_columns(1, rows, **lineage_args())