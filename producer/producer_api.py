"""
Django REST Framework producer -> Kafka.

Single-file Django project on purpose: the whole producer is in one readable
place, which matters when an examiner asks you to explain or rebuild it.

THE DESIGN DECISION YOU WILL BE ASKED ABOUT:
  The Kafka message KEY is driver_id. Kafka hashes the key to pick a
  partition, so every event for a given driver always lands on the same
  partition. Kafka only guarantees ordering WITHIN a partition, so keying by
  driver preserves per-driver ordering - which is the ordering that matters,
  because a driver cannot start trip 2 before finishing trip 1. We do not
  need global ordering across all drivers, which is why 3 partitions is safe.
  Keying by trip_id would spread evenly but give no useful ordering; no key
  at all would round-robin and lose ordering entirely.

WHY THE VALUE LOOKS ODD:
  We publish {"schema": {...}, "payload": {...}} instead of plain JSON. The
  Kafka Connect JDBC sink needs a declared schema to create the MariaDB
  table, and Parquet output needs one too. Send plain JSON and Connect
  rejects every record - which is exactly what happens to the stray
  console-producer test messages sitting in the topic.

Run:
    python producer_api.py runserver 8001

Endpoints:
    POST /api/trips/    publish one trip event
    GET  /api/health/   browsable API status page
"""
import json
import os
import sys

import django
from django.conf import settings
from django.urls import path

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "trip-events")

if not settings.configured:
    settings.configure(
        DEBUG=True,
        SECRET_KEY="dev-only-not-for-production",
        ROOT_URLCONF=__name__,
        ALLOWED_HOSTS=["*"],
        INSTALLED_APPS=[
            "django.contrib.contenttypes",
            "django.contrib.auth",
            "django.contrib.staticfiles",
            "rest_framework",
        ],
        DATABASES={},
        REST_FRAMEWORK={"DEFAULT_AUTHENTICATION_CLASSES": []},
        # APP_DIRS=True lets Django find templates inside installed apps.
        # DRF ships rest_framework/api.html for its browsable API; without
        # this the HTML view raises TemplateDoesNotExist (the JSON API still
        # works, which is why curl succeeded before this was added).
        TEMPLATES=[{
            "BACKEND": "django.template.backends.django.DjangoTemplates",
            "APP_DIRS": True,
            "OPTIONS": {"context_processors": [
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
            ]},
        }],
        STATIC_URL="/static/",
        USE_TZ=True,
    )
    django.setup()

from rest_framework import serializers, status          # noqa: E402
from rest_framework.decorators import api_view          # noqa: E402
from rest_framework.response import Response            # noqa: E402
from kafka import KafkaProducer                         # noqa: E402

FIELDS = [
    ("trip_id", "string"),
    ("driver_id", "string"),
    ("rider_id", "string"),
    ("pickup_zone", "string"),
    ("dropoff_zone", "string"),
    ("distance_km", "double"),
    ("pickup_hour", "int32"),
    ("day_of_week", "int32"),
    ("weather", "string"),
    ("traffic_level", "string"),
    ("surge_multiplier", "double"),
    ("fare_rwf", "double"),
    ("duration_min", "double"),
    ("event_ts", "string"),
]

VALUE_SCHEMA = {
    "type": "struct",
    "name": "trip_event",
    "optional": False,
    "fields": [{"field": f, "type": t, "optional": True} for f, t in FIELDS],
}


class TripSerializer(serializers.Serializer):
    """Validation at the edge: bad records are rejected with HTTP 400 and
    never reach Kafka, so they never reach MariaDB or HDFS either."""
    trip_id = serializers.CharField()
    driver_id = serializers.CharField()
    rider_id = serializers.CharField()
    pickup_zone = serializers.CharField()
    dropoff_zone = serializers.CharField()
    distance_km = serializers.FloatField()
    pickup_hour = serializers.IntegerField()
    day_of_week = serializers.IntegerField()
    weather = serializers.CharField()
    traffic_level = serializers.CharField()
    surge_multiplier = serializers.FloatField()
    fare_rwf = serializers.FloatField()
    duration_min = serializers.FloatField()
    event_ts = serializers.CharField()


_producer = None


def get_producer():
    """One KafkaProducer per process. acks='all' means the leader waits for
    all in-sync replicas before acknowledging: durability over latency.
    This is also why the measured rate is lower than the requested rate -
    every request blocks on an acknowledgement."""
    global _producer
    if _producer is None:
        _producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            key_serializer=lambda k: k.encode("utf-8"),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            retries=3,
            linger_ms=20,
        )
    return _producer


@api_view(["POST"])
def publish_trip(request):
    s = TripSerializer(data=request.data)
    if not s.is_valid():
        return Response({"errors": s.errors}, status=status.HTTP_400_BAD_REQUEST)

    payload = dict(s.validated_data)
    envelope = {"schema": VALUE_SCHEMA, "payload": payload}

    future = get_producer().send(
        KAFKA_TOPIC,
        key=payload["driver_id"],      # partitioning key
        value=envelope,
    )
    meta = future.get(timeout=10)      # block so we can report partition/offset

    return Response(
        {
            "status": "published",
            "topic": meta.topic,
            "partition": meta.partition,
            "offset": meta.offset,
            "key": payload["driver_id"],
        },
        status=status.HTTP_201_CREATED,
    )


@api_view(["GET"])
def health(request):
    return Response({
        "status": "ok",
        "topic": KAFKA_TOPIC,
        "bootstrap": KAFKA_BOOTSTRAP,
        "partitioning_key": "driver_id",
        "acks": "all",
    })


urlpatterns = [
    path("api/trips/", publish_trip),
    path("api/health/", health),
]

if __name__ == "__main__":
    from django.core.management import execute_from_command_line
    execute_from_command_line(sys.argv)