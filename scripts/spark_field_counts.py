"""Count raw fields per line with Spark (the CSV reader hides short lines)."""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SOURCE = "data/raw/electronics/kz.csv"
HEADER = "event_time,order_id,product_id,category_id,category_code,brand,price,user_id"

spark = (
    SparkSession.builder.master("local[*]")
    .appName("CommerceLakeFieldCounts")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

lines = spark.read.text(SOURCE)
data = lines.where(F.col("value") != HEADER)

print("Header lines removed:", lines.count() - data.count())
print("Data lines:", data.count())

counts = (
    data.withColumn("field_count", F.size(F.split(F.col("value"), ",")))
    .groupBy("field_count").count().orderBy("field_count")
)
counts.show()

print("Lines containing a double quote:", data.where(F.col("value").contains('"')).count())
spark.stop()