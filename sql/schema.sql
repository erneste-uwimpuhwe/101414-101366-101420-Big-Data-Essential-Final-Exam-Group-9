-- Operational store (MariaDB). Fast current-state queries for the dashboard.
-- HDFS holds the full replayable history; this holds only what the dashboard
-- needs to answer "what is true right now".

CREATE DATABASE IF NOT EXISTS ridehail;
USE ridehail;

-- `trips` is auto-created by the Kafka Connect JDBC sink from the schema in
-- the message envelope. Defined here for reference and in case auto.create
-- is turned off.
CREATE TABLE IF NOT EXISTS trips (
    trip_id          VARCHAR(64) PRIMARY KEY,
    driver_id        VARCHAR(16),
    rider_id         VARCHAR(16),
    pickup_zone      VARCHAR(64),
    dropoff_zone     VARCHAR(64),
    distance_km      DOUBLE,
    pickup_hour      INT,
    day_of_week      INT,
    weather          VARCHAR(32),
    traffic_level    VARCHAR(32),
    surge_multiplier DOUBLE,
    fare_rwf         DOUBLE,
    duration_min     DOUBLE,
    event_ts         VARCHAR(40),
    INDEX idx_zone (pickup_zone),
    INDEX idx_ts (event_ts)
);

-- Written by the custom consumer group (the business-logic layer).
CREATE TABLE IF NOT EXISTS trip_alerts (
    trip_id       VARCHAR(64) PRIMARY KEY,
    driver_id     VARCHAR(16),
    pickup_zone   VARCHAR(64),
    dropoff_zone  VARCHAR(64),
    distance_km   DOUBLE,
    duration_min  DOUBLE,
    traffic_level VARCHAR(32),
    partition_id  INT,
    kafka_offset  BIGINT,
    detected_at   DATETIME
);

-- Written by spark_jobs/insights.py
CREATE TABLE IF NOT EXISTS insight_zone_demand (
    pickup_zone  VARCHAR(64) PRIMARY KEY,
    trip_count   BIGINT,
    avg_duration DOUBLE,
    avg_distance DOUBLE,
    total_fare   DOUBLE,
    computed_at  DATETIME
);

CREATE TABLE IF NOT EXISTS insight_hourly_demand (
    pickup_hour  INT PRIMARY KEY,
    trip_count   BIGINT,
    avg_duration DOUBLE,
    avg_surge    DOUBLE,
    computed_at  DATETIME
);

CREATE TABLE IF NOT EXISTS insight_traffic_impact (
    traffic_level  VARCHAR(32) PRIMARY KEY,
    trip_count     BIGINT,
    avg_duration   DOUBLE,
    avg_min_per_km DOUBLE,
    computed_at    DATETIME
);

-- Written by spark_jobs/score_batch.py
CREATE TABLE IF NOT EXISTS predictions (
    trip_id       VARCHAR(64) PRIMARY KEY,
    pickup_zone   VARCHAR(64),
    dropoff_zone  VARCHAR(64),
    distance_km   DOUBLE,
    traffic_level VARCHAR(32),
    actual_min    DOUBLE,
    predicted_min DOUBLE,
    error_min     DOUBLE,
    scored_at     DATETIME
);

CREATE TABLE IF NOT EXISTS model_metrics (
    id         INT AUTO_INCREMENT PRIMARY KEY,
    model_name VARCHAR(64),
    rmse       DOUBLE,
    mae        DOUBLE,
    r2         DOUBLE,
    train_rows BIGINT,
    test_rows  BIGINT,
    trained_at DATETIME
);