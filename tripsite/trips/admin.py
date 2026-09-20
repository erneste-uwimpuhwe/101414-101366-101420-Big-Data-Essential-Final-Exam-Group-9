"""
Admin registration for the pipeline's tables.

Each ModelAdmin is read-only: has_add_permission and has_change_permission
return False because Django is not the writer here. Trips arrive through
Kafka Connect, alerts through our consumer group, predictions and insights
through PySpark. Letting an admin edit a row would put the operational store
out of step with the HDFS event history, which is the source of truth.

What the admin buys us over a hand-written page: search, filtering by any
field, date hierarchies, and paging over 400,000+ rows without loading them
all - all for a few lines of declaration.
"""
from django.contrib import admin

from .models import ModelMetric, Prediction, Trip, TripAlert


class ReadOnlyAdmin(admin.ModelAdmin):
    """Browse and filter, never write. See module docstring."""

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return False


@admin.register(Trip)
class TripAdmin(ReadOnlyAdmin):
    list_display = ("trip_id", "driver_id", "pickup_zone", "dropoff_zone",
                    "distance_km", "duration_min", "traffic_level",
                    "weather", "pickup_hour", "surge_multiplier",
                    "fare_rwf", "event_ts")
    list_filter = ("pickup_zone", "traffic_level", "weather", "pickup_hour",
                   "day_of_week")
    search_fields = ("trip_id", "driver_id", "rider_id")
    ordering = ("-event_ts",)
    list_per_page = 50
    # show_full_result_count=False skips the expensive unfiltered COUNT(*)
    # that the admin otherwise runs on every page load. On a table this size
    # that query alone costs seconds.
    show_full_result_count = False


@admin.register(TripAlert)
class TripAlertAdmin(ReadOnlyAdmin):
    list_display = ("driver_id", "pickup_zone", "dropoff_zone", "distance_km",
                    "duration_min", "traffic_level", "partition_id",
                    "kafka_offset", "detected_at")
    list_filter = ("traffic_level", "pickup_zone", "partition_id")
    search_fields = ("trip_id", "driver_id")
    ordering = ("-detected_at",)
    list_per_page = 50
    show_full_result_count = False


@admin.register(Prediction)
class PredictionAdmin(ReadOnlyAdmin):
    list_display = ("pickup_zone", "dropoff_zone", "distance_km",
                    "traffic_level", "actual_min", "predicted_min",
                    "error_min", "scored_at")
    list_filter = ("traffic_level", "pickup_zone")
    search_fields = ("trip_id",)
    ordering = ("-scored_at",)
    list_per_page = 50
    show_full_result_count = False


@admin.register(ModelMetric)
class ModelMetricAdmin(ReadOnlyAdmin):
    list_display = ("model_name", "rmse", "mae", "r2",
                    "train_rows", "test_rows", "trained_at")
    ordering = ("-trained_at",)


admin.site.site_header = "Ride-hailing pipeline"
admin.site.site_title = "Pipeline admin"
admin.site.index_title = "Data arriving through Kafka"