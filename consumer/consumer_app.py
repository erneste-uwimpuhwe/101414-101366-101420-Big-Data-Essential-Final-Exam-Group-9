"""
Custom consumer application - the piece carrying most of the Kafka marks.

WHAT THIS DEMONSTRATES, and what every group member must be able to say:

1. CONSUMER GROUP
   Every instance joins group "trip-workers". Kafka assigns each partition
   to exactly ONE consumer in the group, so work is divided, not duplicated.
   Run two instances in two windows and the split is visible in the logs.

2. REBALANCE
   Stop one instance with Ctrl+C. Kafka notices the departure and reassigns
   its partitions to the survivor - printed by the listener below. Restart
   it and the partitions split again.

3. OFFSET MANAGEMENT - manual commits, at-least-once
   enable_auto_commit=False. We do the work FIRST, then commit. If this
   process dies between doing the work and committing, the next owner of
   that partition re-reads from the last committed offset, so the record is
   processed AGAIN - never lost. That is at-least-once delivery. The cost is
   possible duplicates, which we make harmless by using trip_id as the
   primary key (INSERT ... ON DUPLICATE KEY UPDATE = idempotent write).

   Committing BEFORE the work would be at-most-once: faster, but a crash
   loses records. Exactly-once would need Kafka transactions plus a
   transactional sink; we chose not to pay that cost, and can explain why.

   This was demonstrated accidentally during development: the consumer
   crashed on a malformed record, and on restart resumed from the last
   committed offsets rather than from zero. Nothing was lost, nothing was
   reprocessed from the beginning.

4. BUSINESS LOGIC
   Flags trips that took more than LATE_THRESHOLD x the expected time and
   writes them to trip_alerts, which the dashboard surfaces. This is the
   "custom business logic / notifications" box in the reference diagram.

Inspect the group from another window:
    kafka-consumer-groups.bat --bootstrap-server localhost:9092 ^
        --describe --group trip-workers

Run (two windows, different INSTANCE values):
    set INSTANCE=consumer-1
    python consumer_app.py
"""
import json
import os
import signal
from datetime import datetime

import mysql.connector
from kafka import KafkaConsumer
from kafka.consumer.subscription_state import ConsumerRebalanceListener

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "trip-events")
GROUP_ID = os.getenv("GROUP_ID", "trip-workers")
INSTANCE = os.getenv("INSTANCE", "consumer-1")

# A trip is "late" if it took more than this multiple of the expected time at
# 30 km/h in ideal conditions.
#
# Tuning history, worth explaining: at 1.5x roughly 60% of trips were flagged
# and at 2.0x still 7%, because the generator's traffic and weather factors
# routinely push trips past the ideal-conditions baseline - gridlock alone is
# 2.2x. An alert that fires on ordinary congestion tells a dispatcher nothing
# they do not already know. 3.0x isolates genuine outliers.
#
# The more principled fix would be to compare against expected time GIVEN the
# observed conditions rather than a flat baseline; that is the natural next
# iteration and is noted as a limitation in the report.
LATE_THRESHOLD = float(os.getenv("LATE_THRESHOLD", "3.0"))
EXPECTED_SPEED_KMH = 30.0

MYSQL = dict(
    host=os.getenv("MYSQL_HOST", "127.0.0.1"),
    port=int(os.getenv("MYSQL_PORT", "3306")),
    user=os.getenv("MYSQL_USER", "root"),
    password=os.getenv("MYSQL_PASSWORD", ""),
    database=os.getenv("MYSQL_DB", "ridehail"),
)

running = True


def stop(signum, frame):
    global running
    running = False
    print(f"\n[{INSTANCE}] shutdown requested, finishing current batch...")


signal.signal(signal.SIGINT, stop)


def log(msg):
    print(f"[{datetime.now():%H:%M:%S}] [{INSTANCE}] {msg}", flush=True)


def is_late(trip):
    """Expected time at 30 km/h in ideal conditions vs what actually happened."""
    expected = (trip["distance_km"] / EXPECTED_SPEED_KMH) * 60.0
    return expected > 0 and trip["duration_min"] > expected * LATE_THRESHOLD


def safe_json(raw):
    """Deserialization runs BEFORE our own validation in the loop below, so a
    record that is not JSON at all - the console-producer test messages still
    sitting in the topic - would crash the consumer here. Returning None
    instead lets the loop skip it. Same posture as errors.tolerance=all in
    the Connect sinks: reject malformed records, never halt the pipeline."""
    try:
        return json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return None


class Listener(ConsumerRebalanceListener):
    """Makes rebalances visible during the demo. Kafka calls these hooks
    when partitions move between consumers in the group."""

    def on_partitions_revoked(self, revoked):
        parts = sorted(tp.partition for tp in revoked)
        log(f"PARTITIONS REVOKED -> {parts}  (rebalance starting)")

    def on_partitions_assigned(self, assigned):
        parts = sorted(tp.partition for tp in assigned)
        log(f"PARTITIONS ASSIGNED -> {parts}")


def main():
    conn = mysql.connector.connect(**MYSQL)
    conn.autocommit = False
    cur = conn.cursor()

    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=GROUP_ID,
        enable_auto_commit=False,          # <-- manual commits
        auto_offset_reset="earliest",
        value_deserializer=safe_json,
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        max_poll_records=200,
        session_timeout_ms=10000,
        heartbeat_interval_ms=3000,
    )

    consumer.subscribe([KAFKA_TOPIC], listener=Listener())
    log(f"joined group '{GROUP_ID}' on topic '{KAFKA_TOPIC}' "
        f"(late threshold {LATE_THRESHOLD}x)")

    processed = alerts = skipped = 0
    while running:
        batch = consumer.poll(timeout_ms=1000)
        if not batch:
            continue

        for tp, records in batch.items():
            for rec in records:
                # Malformed or non-envelope records: count and move on.
                if not isinstance(rec.value, dict) or "payload" not in rec.value:
                    skipped += 1
                    continue
                trip = rec.value["payload"]

                if is_late(trip):
                    cur.execute(
                        """INSERT INTO trip_alerts
                           (trip_id, driver_id, pickup_zone, dropoff_zone,
                            distance_km, duration_min, traffic_level,
                            partition_id, kafka_offset, detected_at)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                           ON DUPLICATE KEY UPDATE detected_at = NOW()""",
                        (trip["trip_id"], trip["driver_id"],
                         trip["pickup_zone"], trip["dropoff_zone"],
                         trip["distance_km"], trip["duration_min"],
                         trip["traffic_level"], rec.partition, rec.offset),
                    )
                    alerts += 1
                processed += 1

        # Work for the whole batch is done - only NOW is it safe to commit.
        conn.commit()          # database transaction first
        consumer.commit()      # then Kafka offsets

        offsets = {tp.partition: consumer.position(tp)
                   for tp in consumer.assignment()}
        log(f"committed | processed={processed} alerts={alerts} "
            f"skipped={skipped} next_offsets={dict(sorted(offsets.items()))}")

    log("closing consumer - leaving the group triggers a rebalance")
    consumer.close()
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()