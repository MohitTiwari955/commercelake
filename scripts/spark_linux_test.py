
from pyspark.sql import SparkSession

# 1. Create a Spark session
spark = (
    SparkSession.builder
    .appName("CommerceLake-Linux-Test")
    .master("local[*]")
    .getOrCreate()
)

# 2. Display Spark version
print("Spark version:", spark.version)

# 3. Create a small DataFrame
data = [
    (101, "Mobile", 500),
    (102, "Laptop", 800),
    (103, "Tablet", 300)
]

columns = ["order_id", "product", "price"]

df = spark.createDataFrame(data, columns)

# 4. Display the data
df.show()

# 5. Perform a simple calculation
df.selectExpr("SUM(price) AS total_price").show()

# 6. Stop Spark
spark.stop()
