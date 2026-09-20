"""
Trip event generator.

Posts synthetic ride-hailing trips to the Django REST producer at a tunable
rate. The RATE env var is the 'velocity' knob you change live during the demo.

duration_min is the ML label, and it is generated from a real (noisy)
relationship: base travel time scaled by traffic, weather and rush hour, plus
Gaussian noise. That means the MLlib model has genuine signal to learn, and
the noise term is why R-squared will not be 1.0 - be ready to say that.

Run:
    set RATE=50
    python generate.py
"""
import os
import random
import time
import uuid
from datetime import datetime, timezone

import requests
from faker import Faker

fake = Faker()

PRODUCER_URL = os.getenv("PRODUCER_URL", "http://127.0.0.1:8001/api/trips/")
RATE = float(os.getenv("RATE", "10"))          # records per second
DURATION = float(os.getenv("DURATION", "0"))   # 0 = run until Ctrl+C

ZONES = ["Kacyiru", "Nyamirambo", "Remera", "Kimironko",
         "Gikondo", "Kicukiro", "Nyarugenge", "Gisozi"]
WEATHER = ["clear", "rain", "heavy_rain", "cloudy"]
TRAFFIC = ["light", "moderate", "heavy", "gridlock"]

# Fixed driver pool so key-based partitioning is demonstrable: 60 keys over
# 3 partitions means the same driver always lands on the same partition.
DRIVERS = [f"D{str(i).zfill(4)}" for i in range(1, 61)]

TRAFFIC_FACTOR = {"light": 1.0, "moderate": 1.25, "heavy": 1.6, "gridlock": 2.2}
WEATHER_FACTOR = {"clear": 1.0, "cloudy": 1.03, "rain": 1.18, "heavy_rain": 1.35}

# Hour-of-day demand curve: a morning peak around 08:00 and a larger evening
# peak at 18:00-19:00, quiet overnight. Used instead of the wall-clock hour.
HOUR_WEIGHTS = [1, 1, 1, 1, 2, 4, 8, 14, 16, 10, 7, 7,
                8, 8, 7, 9, 13, 17, 18, 14, 9, 5, 3, 2]
RUSH_HOURS = (7, 8, 17, 18, 19)


def make_trip():
    pickup, dropoff = random.sample(ZONES, 2)
    distance_km = round(random.uniform(1.0, 28.0), 2)
    weather = random.choices(WEATHER, weights=[60, 22, 8, 10])[0]
    traffic = random.choices(TRAFFIC, weights=[30, 38, 24, 8])[0]
    now = datetime.now(timezone.utc)

    # Draw the hour from the demand curve rather than the wall clock. Using
    # now.hour stamped every record with whatever hour we happened to be
    # running the generator, so the hourly insight showed all demand in the
    # evening and pickup_hour carried almost no signal for the model.
    hour = random.choices(range(24), weights=HOUR_WEIGHTS)[0]

    # Same for the weekday, so day_of_week is a real feature rather than a
    # constant for the whole dataset.
    dow = random.choices(range(1, 8),
                         weights=[16, 16, 16, 16, 18, 12, 6])[0]

    rush = 1.2 if hour in RUSH_HOURS else 1.0

    base_speed = 30.0
    duration = (distance_km / base_speed) * 60.0
    duration *= TRAFFIC_FACTOR[traffic] * WEATHER_FACTOR[weather] * rush
    duration += random.gauss(0, 1.8)      # irreducible noise - caps R-squared
    duration = max(2.0, round(duration, 2))

    surge = round(random.choice([1.0, 1.0, 1.0, 1.2, 1.5, 1.8, 2.0]), 2)

    # NOTE: fare is derived FROM duration. It must never be used as a model
    # feature - the model could invert this formula and score suspiciously
    # well, but in production the fare is not known before the trip ends.
    # This is the target-leakage trap in this dataset.
    fare = round(500 + distance_km * 420 * surge + duration * 25, 2)

    return {
        "trip_id": str(uuid.uuid4()),
        "driver_id": random.choice(DRIVERS),
        "rider_id": f"R{random.randint(1, 5000):05d}",
        "pickup_zone": pickup,
        "dropoff_zone": dropoff,
        "distance_km": distance_km,
        "pickup_hour": hour,
        "day_of_week": dow,
        "weather": weather,
        "traffic_level": traffic,
        "surge_multiplier": surge,
        "fare_rwf": fare,
        "duration_min": duration,
        "event_ts": now.isoformat(),
    }


def main():
    interval = 1.0 / RATE if RATE > 0 else 0
    sent = failed = 0
    started = time.time()
    session = requests.Session()

    print(f"Sending to {PRODUCER_URL} at ~{RATE} records/second. Ctrl+C to stop.")
    try:
        while True:
            try:
                r = session.post(PRODUCER_URL, json=make_trip(), timeout=5)
                if r.status_code in (200, 201, 202):
                    sent += 1
                else:
                    failed += 1
                    print(f"  producer returned {r.status_code}: {r.text[:120]}")
            except requests.RequestException as exc:
                failed += 1
                print(f"  request failed: {exc}")

            if sent and sent % 100 == 0:
                elapsed = time.time() - started
                print(f"sent={sent} failed={failed} "
                      f"actual_rate={sent / elapsed:.1f}/s")

            if DURATION and (time.time() - started) >= DURATION:
                break
            time.sleep(interval)
    except KeyboardInterrupt:
        pass
    finally:
        elapsed = time.time() - started or 1
        print(f"\nStopped. sent={sent} failed={failed} "
              f"average_rate={sent / elapsed:.1f}/s over {elapsed:.0f}s")


if __name__ == "__main__":
    main()