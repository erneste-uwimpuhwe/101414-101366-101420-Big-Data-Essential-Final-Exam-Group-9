"""
Spark MLlib: train an ETA model on our own pipeline's data, read from HDFS.

PROBLEM TYPE: supervised regression. Target = duration_min (minutes).

THE MODEL MIRRORS HOW THE DATA IS ACTUALLY GENERATED, which is why these
features and this model were chosen. The generator computes:

    duration = (distance / 30 km/h) * 60
               * traffic_factor      (light 1.0 ... gridlock 2.2)
               * weather_factor      (clear 1.0 ... heavy_rain 1.35)
               * rush_factor         (1.2 in hours 7,8,17,18,19 else 1.0)
               + gauss(0, 1.8)       <- irreducible noise

So the relationship is MULTIPLICATIVE with an interaction between distance
and every scaling factor. That is exactly why we use gradient-boosted trees
rather than linear regression: a linear model can only add contributions, so
it cannot represent "traffic multiplies the effect of distance" without
hand-built interaction terms. Trees split on distance and traffic jointly
and get the interaction for free. We train a linear baseline in the same run
to prove the point with numbers.

The gauss(0, 1.8) term is the ceiling on achievable accuracy. No model can
predict noise, so R2 cannot reach 1.0 and RMSE cannot fall far below about
1.8 minutes. If our RMSE lands near that, the model has learned essentially
everything learnable - and if it came out well BELOW it, that would be
evidence of leakage, not skill.

THE FEATURE WE DELIBERATELY EXCLUDED - expect to be asked:
  fare_rwf is computed FROM duration_min upstream. Including it would let the
  model invert that formula and score near-perfectly, but in production the
  fare is not known until the trip ends. That is target leakage.

WHY WE FILTER ON dt:
  Early records were stamped with the wall-clock hour, so pickup_hour was
  meaningless for them. Corrected records land in a later date partition.
  Filtering on dt makes Spark prune the old directory entirely rather than
  read and discard it - partition pruning, which is what the date-based HDFS
  layout buys us.

WHY THE MODEL IS SMALL AND THE DATA SAMPLED:
  A first attempt at 60 trees x depth 5 over 400k rows starved the driver of
  heartbeat time on this single machine - Spark logged "no recent heartbeats:
  132829 ms exceeds timeout 120000 ms" and killed its own executor. Boosting
  has diminishing returns, so 30 trees at depth 4 over a 20% sample gives
  equivalent metrics in a fraction of the time. On a real cluster neither
  limit would apply.

Run:
    set PYSPARK_PYTHON=C:\\PROGRA~1\\Python313\\python.exe
    set PYSPARK_DRIVER_PYTHON=C:\\PROGRA~1\\Python313\\python.exe
    spark-submit --driver-memory 4g --jars D:\\kafka-connect\\plugins\\confluentinc-kafka-connect-jdbc-10.9.7\\lib\\mariadb-java-client-3.5.10.jar spark_jobs\\train_model.py
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor, LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

HDFS_TRIPS = "hdfs://localhost:9000/data/trip-events"
MODEL_PATH = "hdfs://localhost:9000/models/eta_gbt"
TRAIN_PARTITION = "2026-09-16"
SAMPLE_FRACTION = 0.2
NOISE_SD = 1.8          # the generator's Gaussian noise term

JDBC_URL = ("jdbc:mysql://127.0.0.1:3306/ridehail"
            "?useSSL=false&permitMysqlScheme")
JDBC_PROPS = {"user": "root", "password": "",
              "driver": "org.mariadb.jdbc.Driver"}

CATEGORICAL = ["pickup_zone", "dropoff_zone", "weather", "traffic_level"]
NUMERIC = ["distance_km", "pickup_hour", "day_of_week", "surge_multiplier"]

spark = (SparkSession.builder
         .appName("eta-model-training")
         .config("spark.network.timeout", "600s")
         .config("spark.executor.heartbeatInterval", "60s")
         .config("spark.sql.shuffle.partitions", "16")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

data = (spark.read.parquet(HDFS_TRIPS)
        .filter(F.col("dt") == TRAIN_PARTITION)      # partition pruning
        .dropDuplicates(["trip_id"])
        .filter(F.col("distance_km").between(0.3, 60))
        .filter(F.col("duration_min").between(1, 300))
        .select(*CATEGORICAL, *NUMERIC, "duration_min")
        .dropna()
        .sample(withReplacement=False, fraction=SAMPLE_FRACTION, seed=42)
        .cache())

total = data.count()
print(f"\ntraining rows read from HDFS (dt={TRAIN_PARTITION}, "
      f"{SAMPLE_FRACTION:.0%} sample): {total}")

# 80/20 split, seeded so the split is reproducible: any change in metrics
# between runs then reflects data or model changes, not a different shuffle.
train, test = data.randomSplit([0.8, 0.2], seed=42)
train.cache()
test.cache()
n_train, n_test = train.count(), test.count()
print(f"train={n_train}  test={n_test}  "
      f"(test rows are never seen during training)\n")

indexers = [StringIndexer(inputCol=c, outputCol=f"{c}_idx",
                          handleInvalid="keep") for c in CATEGORICAL]
encoders = [OneHotEncoder(inputCol=f"{c}_idx", outputCol=f"{c}_vec")
            for c in CATEGORICAL]
assembler = VectorAssembler(
    inputCols=[f"{c}_vec" for c in CATEGORICAL] + NUMERIC,
    outputCol="features")

gbt = GBTRegressor(featuresCol="features", labelCol="duration_min",
                   maxIter=30, maxDepth=4, stepSize=0.15,
                   maxBins=32, seed=42)

print("training GBTRegressor (30 trees, depth 4)...")
model = Pipeline(stages=indexers + encoders + [assembler, gbt]).fit(train)
preds = model.transform(test).cache()


def score(df):
    return {m: RegressionEvaluator(labelCol="duration_min",
                                   predictionCol="prediction",
                                   metricName=m).evaluate(df)
            for m in ("rmse", "mae", "r2")}


gbt_metrics = score(preds)
print("\n=== GBT metrics on the held-out test set ===")
for k, v in gbt_metrics.items():
    print(f"  {k.upper():5s} = {v:.3f}")
print(f"  (the generator's noise SD is {NOISE_SD} min - that is the floor "
      f"any model could reach)")

print("\ntraining LinearRegression baseline for comparison...")
lr_metrics = score(
    Pipeline(stages=indexers + encoders + [
        assembler,
        LinearRegression(featuresCol="features", labelCol="duration_min")
    ]).fit(train).transform(test))
print("=== LinearRegression baseline ===")
for k, v in lr_metrics.items():
    print(f"  {k.upper():5s} = {v:.3f}")

print(f"\nGBT reduces RMSE by "
      f"{(1 - gbt_metrics['rmse'] / lr_metrics['rmse']) * 100:.1f}% "
      f"versus the linear baseline - the multiplicative structure of the "
      f"data is why")

print("\n=== sample predictions vs actual (test set) ===")
preds.select(
    F.round("distance_km", 2).alias("km"), "traffic_level", "weather",
    "pickup_hour",
    F.round("duration_min", 2).alias("actual_min"),
    F.round("prediction", 2).alias("predicted_min"),
    F.round(F.col("prediction") - F.col("duration_min"), 2).alias("error")
).show(12, False)

print("=== GBT feature importances (top 8) ===")
importances = model.stages[-1].featureImportances.toArray()
names = assembler.getInputCols()
for idx, imp in sorted(enumerate(importances), key=lambda p: -p[1])[:8]:
    print(f"  feature index {idx:3d}   importance {imp:.4f}")
print(f"\nassembler inputs in order: {names}")
print("(one-hot blocks come first, then the four numeric columns - so the "
      "last four indices are distance_km, pickup_hour, day_of_week, "
      "surge_multiplier)")

model.write().overwrite().save(MODEL_PATH)
print(f"\nmodel saved to {MODEL_PATH}")

(spark.createDataFrame(
    [("GBTRegressor",
      float(gbt_metrics["rmse"]), float(gbt_metrics["mae"]),
      float(gbt_metrics["r2"]), int(n_train), int(n_test))],
    ["model_name", "rmse", "mae", "r2", "train_rows", "test_rows"])
 .withColumn("trained_at", F.current_timestamp())
 .write.mode("append")
 .jdbc(JDBC_URL, "model_metrics", properties=JDBC_PROPS))
print("metrics appended to MariaDB table model_metrics")

spark.stop()