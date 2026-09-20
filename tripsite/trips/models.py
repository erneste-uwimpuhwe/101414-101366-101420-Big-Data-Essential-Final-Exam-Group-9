"""
Read-only models over tables this pipeline's other components own.

managed = False is the important line in each Meta class. It tells Django:
this table already exists, do not create it, do not alter it, do not drop it
in tests. `trips` is created by the Kafka Connect JDBC sink from the schema
carried in each Kafka message; trip_alerts is written by our custom consumer
group; the prediction and metric tables are written by PySpark jobs.

Django is a reader here, not the schema owner. Registering these in the
admin gives us a searchable, filterable, paginated browser over data that
arrived through Kafka - without Django ever claiming to manage it.
"""
from django.db import models


class Trip(models.Model):
    trip_id = models.CharField(max_length=64, primary_key=True)
    driver_id = models.CharField(max_length=16, null=True)
    rider_id = models.CharField(max_length=16, null=True)
    pickup_zone = models.CharField(max_length=64, null=True)
    dropoff_zone = models.CharField(max_length=64, null=True)
    distance_km = models.FloatField(null=True)
    pickup_hour = models.IntegerField(null=True)
    day_of_week = models.IntegerField(null=True)
    weather = models.CharField(max_length=32, null=True)
    traffic_level = models.CharField(max_length=32, null=True)
    surge_multiplier = models.FloatField(null=True)
    fare_rwf = models.FloatField(null=True)
    duration_min = models.FloatField(null=True)
    event_ts = models.CharField(max_length=40, null=True)

    class Meta:
        managed = False
        db_table = "trips"
        verbose_name = "trip"
        verbose_name_plural = "trips (from Kafka Connect)"

    def __str__(self):
        return f"{self.driver_id} {self.pickup_zone}->{self.dropoff_zone}"


class TripAlert(models.Model):
    trip_id = models.CharField(max_length=64, primary_key=True)
    driver_id = models.CharField(max_length=16, null=True)
    pickup_zone = models.CharField(max_length=64, null=True)
    dropoff_zone = models.CharField(max_length=64, null=True)
    distance_km = models.FloatField(null=True)
    duration_min = models.FloatField(null=True)
    traffic_level = models.CharField(max_length=32, null=True)
    partition_id = models.IntegerField(null=True)
    kafka_offset = models.BigIntegerField(null=True)
    detected_at = models.DateTimeField(null=True)

    class Meta:
        managed = False
        db_table = "trip_alerts"
        verbose_name = "late-trip alert"
        verbose_name_plural = "late-trip alerts (from consumer group)"

    def __str__(self):
        return f"{self.driver_id} {self.duration_min}min"


class Prediction(models.Model):
    trip_id = models.CharField(max_length=64, primary_key=True)
    pickup_zone = models.CharField(max_length=64, null=True)
    dropoff_zone = models.CharField(max_length=64, null=True)
    distance_km = models.FloatField(null=True)
    traffic_level = models.CharField(max_length=32, null=True)
    actual_min = models.FloatField(null=True)
    predicted_min = models.FloatField(null=True)
    error_min = models.FloatField(null=True)
    scored_at = models.DateTimeField(null=True)

    class Meta:
        managed = False
        db_table = "predictions"
        verbose_name = "prediction"
        verbose_name_plural = "predictions (from Spark MLlib)"

    def __str__(self):
        return f"{self.predicted_min} vs {self.actual_min}"


class ModelMetric(models.Model):
    id = models.AutoField(primary_key=True)
    model_name = models.CharField(max_length=64, null=True)
    rmse = models.FloatField(null=True)
    mae = models.FloatField(null=True)
    r2 = models.FloatField(null=True)
    train_rows = models.BigIntegerField(null=True)
    test_rows = models.BigIntegerField(null=True)
    trained_at = models.DateTimeField(null=True)

    class Meta:
        managed = False
        db_table = "model_metrics"
        verbose_name = "model metric"
        verbose_name_plural = "model metrics"

    def __str__(self):
        return f"{self.model_name} RMSE={self.rmse}"