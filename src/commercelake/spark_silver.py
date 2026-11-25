"""Silver in Spark: type, realign, flag and quarantine Bronze rows. Bronze is never changed."""
from pyspark.sql import Window
from pyspark.sql import functions as F

from commercelake.bronze import SOURCE_COLUMNS

PLACEHOLDER_TIME = "1970-01-01 00:33:40 UTC"
TIME_FORMAT = "yyyy-MM-dd HH:mm:ss 'UTC'"
VALID_FROM = "2020-01-01 00:00:00"
VALID_UNTIL = "2021-01-01 00:00:00"
MONEY = "decimal(18,4)"
LINEAGE_KEEP = ["_source_file", "_source_row_number", "_ingested_at", "_batch_id", "_file_sha256"]
BUSINESS_COLUMNS = ["event_time", "order_id", "product_id", "category_id","category_code", "brand", "price", "user_id"]

def blank_to_null(column):
    """Empty text becomes a real null."""
    return F.when(column != "", column)


def clean_bronze_spark(bronze):
    """Turn a Bronze DataFrame into (silver, quarantine).

    Every Bronze row goes to exactly one of them. The Spark session must use
    the UTC time zone (spark.sql.session.timeZone = UTC).
    """
    c = {name: blank_to_null(F.col(name)) for name in SOURCE_COLUMNS}

    # Lines with only 6 fields have price in category_code and user_id in brand
    short = F.col("_field_count") == 6
    price = F.when(short, c["category_code"]).otherwise(c["price"]).try_cast(MONEY)
    user_id = F.when(short, c["brand"]).otherwise(c["user_id"])
    category_code = F.when(~short, c["category_code"])
    brand = F.when(~short, c["brand"])

    event_time = F.try_to_timestamp(c["event_time"], F.lit(TIME_FORMAT))
    is_placeholder = F.col("event_time").eqNullSafe(F.lit(PLACEHOLDER_TIME))
    in_range = F.coalesce(
        (event_time >= F.lit(VALID_FROM).cast("timestamp"))
        & (event_time < F.lit(VALID_UNTIL).cast("timestamp")),
        F.lit(False),
    )

    short_price_ok = c["category_code"].try_cast(MONEY).isNotNull()
    short_user_ok = c["brand"].isNull() | c["brand"].rlike("^[0-9]{15,}$")

    # Same checks, same order as the Pandas Silver: the first failing check wins
    checks = [
        ("unexpected_field_count", ~F.col("_field_count").isin(6, 8)),
        ("short_line_unexpected_layout", short & ~(short_price_ok & short_user_ok)),
        ("missing_order_id", c["order_id"].isNull()),
        ("missing_product_id", c["product_id"].isNull()),
        ("bad_or_missing_price", price.isNull()),
        ("negative_price", F.coalesce(price < 0, F.lit(False))),
        ("event_time_unusable", ~(in_range | is_placeholder)),
    ]
    reason = F.coalesce(*[F.when(cond, F.lit(name)) for name, cond in checks])
    work = bronze.withColumn("_reason", reason)

    silver = work.where(F.col("_reason").isNull()).select(
        F.when(in_range, event_time).alias("event_time"),
        c["order_id"].alias("order_id"),
        c["product_id"].alias("product_id"),
        c["category_id"].alias("category_id"),
        category_code.alias("category_code"),
        brand.alias("brand"),
        price.alias("price"),
        user_id.alias("user_id"),
        short.alias("was_realigned"),
        is_placeholder.alias("event_time_is_placeholder"),
        F.coalesce(price == 0, F.lit(False)).alias("price_is_zero"),
        c["category_id"].isNull().alias("is_uncatalogued_item"),
        *LINEAGE_KEEP,
    )
    # Quarantine keeps the ORIGINAL Bronze values plus the reason
    quarantine = work.where(F.col("_reason").isNotNull()).select(
        *bronze.columns, F.col("_reason").alias("quarantine_reason")
    )
    return silver, quarantine

def add_exact_repeat_flag(silver):
    """Flag every copy after the first. Compares the WHOLE dataset, not one partition."""
    first_copy_first = Window.partitionBy(*BUSINESS_COLUMNS).orderBy("_source_row_number")
    return silver.withColumn("is_exact_repeat", F.row_number().over(first_copy_first) > 1)
