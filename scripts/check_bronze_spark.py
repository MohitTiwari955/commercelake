"""Check Spark Bronze and compare it row by row with the Pandas Bronze."""
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from commercelake.bronze import SOURCE_COLUMNS

spark = SparkSession.builder.master("local[*]").appName("CheckBronzeSpark").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

spark_load = sorted(Path("data/bronze_spark/electronics_purchases").glob("sha256-*"))[0]
pandas_load = sorted(Path("data/bronze/electronics_purchases").glob("sha256-*"))[0]
print("Spark load: ", spark_load.name)
print("Pandas load:", pandas_load.name)

sb = spark.read.parquet(str(spark_load / "part-*.parquet"))
pb = spark.read.parquet(str(pandas_load / "part-*.parquet"))

print("Part files:", len(list(spark_load.glob("part-*.parquet"))))
print("Rows in Spark Bronze:", sb.count())
print(sb.agg(F.min("_source_row_number"), F.max("_source_row_number"),
             F.countDistinct("_source_row_number")).first())
sb.groupBy("_field_count").count().orderBy("_field_count").show()
print("Distinct batch IDs:", sb.select("_batch_id").distinct().count())
print("Distinct fingerprints:", sb.select("_file_sha256").distinct().count())

# Row-by-row comparison with the verified Pandas Bronze
joined = sb.alias("s").join(
    pb.alias("p"), F.col("s._source_row_number") == F.col("p._source_row_number")
)
differs = F.lit(False)
for name in SOURCE_COLUMNS + ["_field_count", "_file_sha256"]:
    differs = differs | ~F.col("s." + name).eqNullSafe(F.col("p." + name))

print("Rows matched by row number:", joined.count())
print("Rows where any source column differs:", joined.where(differs).count())
spark.stop()