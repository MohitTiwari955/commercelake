import os
import sys

from pyspark.sql import SparkSession

# Make sure Spark uses the same Python as this project
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("commercelake-smoke-test")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
print("Spark version:", spark.version)

# 1. A tiny table made in memory
rows = [("chair", 0.0), ("phone", 162.0), ("cable", 0.0), ("tablet", 250.0)]
df = spark.createDataFrame(rows, ["product", "price"])
df.show()
print("Zero-price rows:", df.filter(df.price == 0).count())

# 2. Can Spark write and read Parquet here? (the step that often fails on Windows)
df.write.mode("overwrite").parquet("data/scratch/spark_smoke_test")
back = spark.read.parquet("data/scratch/spark_smoke_test")
print("Read back from Parquet:", back.count(), "rows")

# 3. Read our real raw CSV, every column as text
raw = spark.read.option("header", True).csv("data/raw/electronics/kz.csv")
print("Raw CSV rows read by Spark:", raw.count())

spark.stop()