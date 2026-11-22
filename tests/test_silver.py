from datetime import datetime, timezone

import pandas as pd

from commercelake.bronze import make_bronze_columns
from commercelake.silver import clean_bronze

FULL = ["2020-04-24 11:50:39 UTC", "100", "555", "777", "electronics.tablet", "samsung", "162.01", "999"]
SHORT = ["2020-05-02 11:12:20 UTC", "101", "556", "", "0.02", "1515915625453675584"]


def bronze_from(rows):
    columns = make_bronze_columns(
        1, rows, "tiny.csv", "abc", "b1", datetime(2026, 1, 1, tzinfo=timezone.utc)
    )
    return pd.DataFrame(columns)


def changed(row, position, value):
    row = list(row)
    row[position] = value
    return row


def test_full_row_is_typed_and_kept():
    silver, quarantine = clean_bronze(bronze_from([FULL]))
    assert len(quarantine) == 0
    row = silver.iloc[0]
    assert row["price"] == 162.01
    assert row["event_time"] == pd.Timestamp("2020-04-24 11:50:39", tz="UTC")
    assert not row["was_realigned"]


def test_short_line_is_realigned():
    silver, quarantine = clean_bronze(bronze_from([SHORT]))
    row = silver.iloc[0]
    assert len(quarantine) == 0
    assert row["price"] == 0.02
    assert row["user_id"] == "1515915625453675584"
    assert pd.isna(row["category_code"])
    assert pd.isna(row["brand"])
    assert row["was_realigned"]
    assert row["is_uncatalogued_item"]


def test_placeholder_time_is_flagged_not_dropped():
    placeholder = changed(FULL, 0, "1970-01-01 00:33:40 UTC")
    silver, quarantine = clean_bronze(bronze_from([placeholder]))
    assert len(quarantine) == 0
    assert pd.isna(silver.iloc[0]["event_time"])
    assert silver.iloc[0]["event_time_is_placeholder"]


def test_impossible_time_goes_to_quarantine():
    old = changed(FULL, 0, "2019-12-31 23:59:59 UTC")
    silver, quarantine = clean_bronze(bronze_from([old]))
    assert len(silver) == 0
    assert quarantine.iloc[0]["quarantine_reason"] == "event_time_unusable"


def test_short_line_with_text_in_price_slot_goes_to_quarantine():
    odd = changed(SHORT, 4, "not-a-price")
    silver, quarantine = clean_bronze(bronze_from([odd]))
    assert len(silver) == 0
    assert quarantine.iloc[0]["quarantine_reason"] == "short_line_unexpected_layout"


def test_zero_price_is_flagged_and_negative_price_is_quarantined():
    zero = changed(FULL, 6, "0")
    negative = changed(FULL, 6, "-5")
    silver, quarantine = clean_bronze(bronze_from([zero, negative]))
    assert len(silver) == 1 and silver.iloc[0]["price_is_zero"]
    assert quarantine.iloc[0]["quarantine_reason"] == "negative_price"


def test_every_bronze_row_ends_up_in_exactly_one_table():
    rows = [FULL, SHORT, changed(FULL, 0, "1970-01-01 00:33:40 UTC"),
            changed(FULL, 0, "2019-01-01 00:00:00 UTC"), changed(FULL, 1, "")]
    silver, quarantine = clean_bronze(bronze_from(rows))
    assert len(silver) + len(quarantine) == len(rows)