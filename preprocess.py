from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("HW4Preprocess")
    .master("local[1]")
    .config("spark.driver.memory", "1g")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

df = spark.read.csv("taxi_trips_clean.csv", header=True, inferSchema=True)

df = df.withColumn("fare_per_minute", df["fare"] / (df["trip_seconds"] / 60.0))

df.createOrReplaceTempView("trips")

result = spark.sql("""
SELECT company, COUNT(*) AS trip_count, ROUND(AVG(fare), 2) AS avg_fare,
ROUND(AVG(fare_per_minute), 2) AS avg_fare_per_minute
FROM trips
GROUP BY company
ORDER BY trip_count DESC
""")

result.write.json("processed_data/", mode="overwrite")
