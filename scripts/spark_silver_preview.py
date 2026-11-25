"""Preview the Spark Silver rules on the Spark Bronze. Nothing is written."""
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from commercelake.spark_silver import clean_bronze_spark

spark = (
    SparkSession.builder.master("local[*]")
    .appName("CommerceLakeSilverPreview")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

load = sorted(Path("data/bronze_spark/electronics_purchases").glob("sha256-*"))[0]
bronze = spark.read.parquet(str(load / "part-*.parquet"))

silver, quarantine = clean_bronze_spark(bronze)
silver.cache()

print("Bronze rows:    ", bronze.count())
print("Silver rows:    ", silver.count())
print("Quarantine rows:", quarantine.count())

flags = ["was_realigned", "event_time_is_placeholder", "price_is_zero", "is_uncatalogued_item"]
silver.agg(
    *[F.sum(F.col(f).cast("int")).alias(f) for f in flags],
    F.sum(F.col("event_time").isNull().cast("int")).alias("event_time_null"),
).show(truncate=False)

silver.printSchema()
silver.where("was_realigned").select(
    "order_id", "category_code", "brand", "price", "user_id"
).show(3, truncate=False)
spark.stop()