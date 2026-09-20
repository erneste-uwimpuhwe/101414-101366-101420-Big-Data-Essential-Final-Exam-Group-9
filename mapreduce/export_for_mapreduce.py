"""
Export a tab-delimited text copy of the trip data for Hadoop Streaming.

WHY THIS STEP EXISTS - and it is part of the comparison, not a workaround:
Hadoop Streaming feeds each input LINE to a process on stdin. It has no
columnar reader, so it cannot read the Parquet our pipeline writes. Spark
reads that Parquet natively and only touches the columns it needs.

So MapReduce needs a preparation step that Spark does not, and the exported
text is far larger than the Parquet it came from - a direct, measurable
demonstration of why columnar storage matters.

Run:
    spark-submit spark_jobs\\..\\mapreduce\\export_for_mapreduce.py
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

HDFS_TRIPS = "hdfs://localhost:9000/data/trip-events"
OUT_TSV = "hdfs://localhost:9000/data/trips_text"

spark = (SparkSession.builder
         .appName("export-text-for-mapreduce")
         .config("spark.sql.shuffle.partitions", "8")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

trips = (spark.read.parquet(HDFS_TRIPS)
         .dropDuplicates(["trip_id"])
         .filter(F.col("distance_km").between(0.3, 60))
         .filter(F.col("duration_min").between(1, 300))
         .select("trip_id", "driver_id", "pickup_zone", "dropoff_zone",
                 "distance_km", "traffic_level", "weather", "duration_min"))

n = trips.count()
print(f"\nexporting {n} cleaned rows as tab-delimited text")

(trips.write.mode("overwrite")
 .option("sep", "\t")
 .option("header", "false")
 .csv(OUT_TSV))

print(f"written to {OUT_TSV}")
print("\nCompare the sizes yourself - this is a finding for the report:")
print("  hdfs dfs -du -s -h /data/trip-events   (Parquet, columnar)")
print("  hdfs dfs -du -s -h /data/trips_text    (text, row-oriented)")

spark.stop()