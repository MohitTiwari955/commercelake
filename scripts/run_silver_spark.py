from pathlib import Path

from pyspark.sql import SparkSession

from commercelake.spark_silver_job import build_silver_spark

spark = (
    SparkSession.builder.master("local[*]")
    .appName("CommerceLakeSilverSpark")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

bronze_root = Path("data/bronze_spark/electronics_purchases")
bronze_load = sorted(bronze_root.glob("sha256-*"))[0]

summary = build_silver_spark(spark, bronze_load, "data/silver_spark/electronics_purchases")
for key, value in summary.items():
    print("%-16s %s" % (key, value))
spark.stop()