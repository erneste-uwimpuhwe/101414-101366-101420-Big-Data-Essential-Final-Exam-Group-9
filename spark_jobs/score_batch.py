"""
Deployment step: load the trained model from HDFS and score recent trips.

This is what makes the model DEPLOYED rather than merely trained: a separate
job, in a separate process, that loads the persisted PipelineModel out of
HDFS and produces predictions on records it has never seen. Nothing about
training is repeated here - the fitted StringIndexers, OneHotEncoders,
VectorAssembler and 30 boosted trees all come back from disk exactly as they
were saved.

WHY IT WRITES A CSV INSTEAD OF STRAIGHT TO MARIADB:
  Spark's JDBC writer spawns a Python worker per partition on this machine,
  and those workers time out connecting back to the driver on Windows
  ("Python worker failed to connect back"). Writing a CSV from Spark and
  loading it with a plain Python client sidesteps that entirely. The
  separation is defensible in its own right: the scoring job produces an
  artifact, and a small loader publishes it to the operational store.

Run:
    spark-submit --driver-memory 4g spark_jobs\\score_batch.py
    python spark_jobs\\load_predictions.py
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import PipelineModel

HDFS_TRIPS = "hdfs://localhost:9000/data/trip-events"
MODEL_PATH = "hdfs://localhost:9000/models/eta_gbt"
OUT_CSV = "file:///D:/bigdata-final/docs/predictions_out"
SCORE_PARTITION = "2026-09-18"
LIMIT = 2000

spark = (SparkSession.builder
         .appName("eta-batch-scoring")
         .config("spark.network.timeout", "600s")
         .config("spark.sql.shuffle.partitions", "8")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

model = PipelineModel.load(MODEL_PATH)
print(f"\nloaded trained model from {MODEL_PATH}")
print(f"pipeline stages: {[type(s).__name__ for s in model.stages]}\n")

recent = (spark.read.parquet(HDFS_TRIPS)
          .filter(F.col("dt") == SCORE_PARTITION)
          .dropDuplicates(["trip_id"])
          .filter(F.col("distance_km").between(0.3, 60))
          .filter(F.col("duration_min").between(1, 300))
          .orderBy(F.desc("event_ts"))
          .limit(LIMIT))

print(f"scoring the {LIMIT} most recent trips...")

scored = (model.transform(recent)
          .select(
              "trip_id", "pickup_zone", "dropoff_zone", "distance_km",
              "traffic_level",
              F.round("duration_min", 2).alias("actual_min"),
              F.round("prediction", 2).alias("predicted_min"))
          .withColumn("error_min",
                      F.round(F.col("predicted_min") - F.col("actual_min"), 2))
          .cache())

scored.show(15, False)

stats = scored.select(
    F.count("*").alias("n"),
    F.round(F.avg(F.abs("error_min")), 3).alias("mae"),
    F.round(F.sqrt(F.avg(F.pow("error_min", 2))), 3).alias("rmse"),
    F.round(F.min("error_min"), 2).alias("worst_under"),
    F.round(F.max("error_min"), 2).alias("worst_over"),
).first()

print(f"\nscored {stats['n']} trips")
print(f"  MAE on this batch : {stats['mae']} minutes")
print(f"  RMSE on this batch: {stats['rmse']} minutes")
print(f"  largest underestimate: {stats['worst_under']} min")
print(f"  largest overestimate : {stats['worst_over']} min")

# coalesce(1) so we get a single CSV file rather than one per partition
(scored.coalesce(1)
 .write.mode("overwrite")
 .option("header", "true")
 .csv(OUT_CSV))
print(f"\npredictions written to {OUT_CSV}")
print("now run: python spark_jobs\\load_predictions.py")

spark.stop()