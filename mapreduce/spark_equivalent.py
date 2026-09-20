"""
The PySpark equivalent of the MapReduce job, for the bonus comparison.

Computes exactly what mapper.py + reducer.py compute: trip count, average
duration and average minutes-per-km grouped by traffic_level. Reads the same
text input from HDFS so the comparison is like-for-like - no Parquet
advantage, no extra insights, no JDBC write.

The entire aggregation is the four lines marked below. The MapReduce version
needs two separate programs, a shuffle phase, and a hand-written loop that
watches for the sort key to change.

Run:
    spark-submit mapreduce\\spark_equivalent.py
"""
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, StringType, StructField,
                               StructType)

TEXT_INPUT = "hdfs://localhost:9000/data/trips_text"

schema = StructType([
    StructField("trip_id", StringType()),
    StructField("driver_id", StringType()),
    StructField("pickup_zone", StringType()),
    StructField("dropoff_zone", StringType()),
    StructField("distance_km", DoubleType()),
    StructField("traffic_level", StringType()),
    StructField("weather", StringType()),
    StructField("duration_min", DoubleType()),
])

spark = SparkSession.builder.appName("spark-equivalent-of-mapreduce").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

started = time.time()

df = (spark.read
      .option("sep", "\t")
      .schema(schema)
      .csv(TEXT_INPUT))

# ------- THE ENTIRE AGGREGATION: four lines -------
result = (df.filter((F.col("distance_km") > 0) & (F.col("duration_min") > 0))
          .groupBy("traffic_level")
          .agg(F.count("*").alias("trip_count"),
               F.round(F.avg("duration_min"), 2).alias("avg_duration"),
               F.round(F.avg(F.col("duration_min") / F.col("distance_km")), 3)
                .alias("avg_min_per_km"))
          .orderBy(F.desc("avg_min_per_km")))
# --------------------------------------------------

result.show(10, False)

n = df.count()
elapsed = time.time() - started

print(f"\nrows processed : {n}")
print(f"wall-clock time: {elapsed:.1f} seconds")
print("\nSame input, same output as the Hadoop Streaming job in this folder.")

spark.stop()