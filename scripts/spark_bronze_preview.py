"""Preview Bronze-shaped columns built with Spark. Nothing is written."""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

SOURCE = "data/raw/electronics/kz.csv"
COLUMNS = ["event_time", "order_id", "product_id", "category_id",
           "category_code", "brand", "price", "user_id"]
HEADER = ",".join(COLUMNS)

spark = SparkSession.builder.master("local[*]").appName("CommerceLakeBronzePreview").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# 1. Number every line in file order (line 0 is the header)
lines = spark.read.text(SOURCE)
schema = StructType([StructField("value", StringType()), StructField("line_number", LongType())])
numbered = spark.createDataFrame(
    lines.rdd.zipWithIndex().map(lambda pair: (pair[0]["value"], pair[1])), schema
)
assert numbered.where("line_number = 0").first()["value"] == HEADER, "unexpected header"

# 2. Split each line into raw fields; a missing field becomes null
fields = F.split(F.col("value"), ",", -1)
bronze = numbered.where("line_number > 0").select(
    *[F.try_element_at(fields, F.lit(i + 1)).alias(name) for i, name in enumerate(COLUMNS)],
    F.size(fields).alias("_field_count"),
    F.col("line_number").alias("_source_row_number"),
)
bronze.cache()

# 3. Checks
print("Rows:", bronze.count())
print(bronze.agg(F.min("_source_row_number"), F.max("_source_row_number"),
                 F.countDistinct("_source_row_number")).first())
bronze.groupBy("_field_count").count().orderBy("_field_count").show()
print("Short rows with null price AND null user_id:",
      bronze.where("_field_count = 6 AND price IS NULL AND user_id IS NULL").count())
print("Eight-field rows with a null price:",
      bronze.where("_field_count = 8 AND price IS NULL").count())
bronze.where("_source_row_number <= 2").show(truncate=False)
spark.stop()