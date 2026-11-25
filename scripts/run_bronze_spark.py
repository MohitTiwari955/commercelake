from pyspark.sql import SparkSession

from commercelake.spark_bronze import build_bronze_spark

spark = SparkSession.builder.master("local[*]").appName("CommerceLakeBronzeSpark").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

summary = build_bronze_spark(spark, "data/raw/electronics/kz.csv", "data/bronze_spark/electronics_purchases")
for key, value in summary.items():
    print("%-10s %s" % (key, value))
spark.stop()