"""Check Spark Silver and compare it row by row with the Pandas Silver."""
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder.master("local[*]")
    .appName("CheckSilverSpark")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

spark_load = sorted(Path("data/silver_spark/electronics_purchases").glob("sha256-*"))[0]
pandas_load = sorted(Path("data/silver/electronics_purchases").glob("sha256-*"))[0]
print("Spark load: ", spark_load.name)
print("Pandas load:", pandas_load.name)

ss = spark.read.parquet(str(spark_load / "silver" / "part-*.parquet"))
ps = spark.read.parquet(str(pandas_load / "silver" / "part-*.parquet"))
print("Spark Silver rows: ", ss.count())
print("Pandas Silver rows:", ps.count())

COMPARE = [
    "event_time", "order_id", "product_id", "category_id", "category_code",
    "brand", "price", "user_id", "was_realigned", "event_time_is_placeholder",
    "price_is_zero", "is_uncatalogued_item", "is_exact_repeat",
]


def differs(name):
    """True where the Spark value and the Pandas value are not the same (null = null counts as same)."""
    if name == "price":
        return ~F.col("s.price").eqNullSafe(F.col("p.price").cast("decimal(18,4)"))
    return ~F.col("s." + name).eqNullSafe(F.col("p." + name))


joined = ss.alias("s").join(
    ps.alias("p"), F.col("s._source_row_number") == F.col("p._source_row_number")
)
result = joined.agg(
    F.count(F.lit(1)).alias("rows_matched"),
    *[F.sum(differs(name).cast("int")).alias(name) for name in COMPARE],
).first()

print("Rows matched by row number:", result["rows_matched"])
print("Rows that differ, per column:")
for name in COMPARE:
    print("  %-26s %d" % (name, result[name] or 0))
spark.stop()