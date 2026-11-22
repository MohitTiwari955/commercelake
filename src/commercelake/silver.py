"""Silver layer: clean, type and flag Bronze rows. Bronze itself is never changed."""
import pandas as pd

from commercelake.bronze import SOURCE_COLUMNS

PLACEHOLDER_TIME = "1970-01-01 00:33:40 UTC"
TIME_FORMAT = "%Y-%m-%d %H:%M:%S UTC"
VALID_FROM = pd.Timestamp("2020-01-01", tz="UTC")
VALID_UNTIL = pd.Timestamp("2021-01-01", tz="UTC")

LINEAGE_KEEP = ["_source_file", "_source_row_number", "_ingested_at", "_batch_id", "_file_sha256"]


def blank_to_null(series):
    """Empty text becomes a real null."""
    return series.mask(series == "")


def looks_like_id(value):
    """True for a long string of digits, such as a user ID."""
    return isinstance(value, str) and value.isdigit() and len(value) >= 15


def clean_bronze(bronze):
    """Turn Bronze rows (all text) into Silver rows.

    Returns two tables: (silver, quarantine). Every Bronze row goes to exactly one of them.
    """
    df = bronze.copy()
    for name in SOURCE_COLUMNS:
        df[name] = blank_to_null(df[name])

    # Lines with only 6 fields have price in category_code and user_id in brand
    short = df["_field_count"] == 6
    price_text = df["price"].where(~short, df["category_code"])
    user_id = df["user_id"].where(~short, df["brand"])
    category_code = df["category_code"].where(~short)
    brand = df["brand"].where(~short)

    price = pd.to_numeric(price_text, errors="coerce")
    event_time = pd.to_datetime(df["event_time"], format=TIME_FORMAT, utc=True, errors="coerce")
    is_placeholder = df["event_time"] == PLACEHOLDER_TIME
    in_range = (event_time >= VALID_FROM) & (event_time < VALID_UNTIL)

    short_price_ok = pd.to_numeric(df["category_code"], errors="coerce").notna()
    short_user_ok = df["brand"].isna() | df["brand"].map(looks_like_id)

    # Quarantine checks: the first one that fails gives the reason
    checks = {
        "unexpected_field_count": ~df["_field_count"].isin([6, 8]),
        "short_line_unexpected_layout": short & ~(short_price_ok & short_user_ok),
        "missing_order_id": df["order_id"].isna(),
        "missing_product_id": df["product_id"].isna(),
        "bad_or_missing_price": price.isna(),
        "negative_price": price < 0,
        "event_time_unusable": ~(in_range | is_placeholder),
    }
    reason = pd.Series(None, index=df.index, dtype="object")
    for name, failed in checks.items():
        reason = reason.mask(reason.isna() & failed, name)

    silver = pd.DataFrame(
        {
            "event_time": event_time.where(in_range),
            "order_id": df["order_id"],
            "product_id": df["product_id"],
            "category_id": df["category_id"],
            "category_code": category_code,
            "brand": brand,
            "price": price,
            "user_id": user_id,
            "was_realigned": short,
            "event_time_is_placeholder": is_placeholder,
            "price_is_zero": price == 0,
            "is_uncatalogued_item": df["category_id"].isna(),
        }
    )
    for name in LINEAGE_KEEP:
        silver[name] = df[name]

    bad = reason.notna()
    quarantine = bronze[bad].copy()
    quarantine["quarantine_reason"] = reason[bad]
    silver = silver[~bad].reset_index(drop=True)
    silver["is_exact_repeat"] = silver.duplicated(subset=BUSINESS_COLUMNS, keep="first")
    return silver, quarantine.reset_index(drop=True)

BUSINESS_COLUMNS = [
    "event_time", "order_id", "product_id", "category_id",
    "category_code", "brand", "price", "user_id",
]