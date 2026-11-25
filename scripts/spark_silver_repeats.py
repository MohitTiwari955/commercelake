"""Count exact repeats in Spark Silver using the whole dataset."""
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from commercelake.spark_silver import add_exact_repeat_flag, clean_bronze_spark

spark = (
    SparkSession.builder.master("local[*]")
    .appName("CommerceLakeSilverRepeats")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

load = sorted(Path("data/bronze_spark/electronics_purchases").glob("sha256-*"))[0]
bronze = spark.read.parquet(str(load / "part-*.parquet"))

silver, quarantine = clean_bronze_spark(bronze)
flagged = add_exact_repeat_flag(silver)
flagged.cache()

print("Silver rows:", flagged.count())
print("Exact repeats:", flagged.where("is_exact_repeat").count())

flagged.where("_source_row_number <= 4").select(
    "_source_row_number", "order_id", "price", "is_exact_repeat"
).orderBy("_source_row_number").show()
spark.stop()