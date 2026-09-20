@echo off
rem ===================================================================
rem  watch_data.cmd - see the actual RECORDS, not just the counts.
rem
rem  watch_growth.cmd answers "is the pipeline moving?"
rem  This one answers "what is moving through it?"
rem
rem  Shows, refreshing every 10 seconds:
rem    [1] the newest trips that landed in MariaDB, field by field
rem    [2] the newest late-trip alerts the consumer group flagged, with
rem        the Kafka partition and offset that produced each
rem    [3] a RANDOM sample of model predictions, plus summary metrics
rem
rem  WHY SECTION 3 SAMPLES RANDOMLY:
rem  The first version sorted predictions by smallest absolute error,
rem  which showed six rows matching to 0.00 and made the model look like
rem  it was reading the answer rather than predicting it. Out of 2,000
rem  scored trips a handful will always land that close, so sorting by
rem  best error is actively misleading. A random sample shows the real
rem  spread, and the summary line underneath gives the honest figure:
rem  MAE around 2.19 min against a generator noise floor of 1.8 min.
rem
rem  RUN IN A FRESH WINDOW. Ctrl+C to stop.
rem ===================================================================
setlocal enabledelayedexpansion
title LIVE DATA - what is flowing through the pipeline

set INTERVAL=10

:loop
cls
echo ===============================================================================
echo   RWANDARIDE - LIVE DATA FEED                     %date%  %time:~0,8%
echo ===============================================================================
echo.
echo  [1] NEWEST TRIPS IN MARIADB  ^(written by the Kafka Connect JDBC sink^)
echo      these arrived seconds ago - the list changes on every refresh
echo  -------------------------------------------------------------------------------
mysql --skip-ssl -u root -e "SELECT LEFT(trip_id,8) AS trip, driver_id AS driver, CONCAT(pickup_zone,' > ',dropoff_zone) AS route, distance_km AS km, ROUND(duration_min,1) AS mins, traffic_level AS traffic, weather, ROUND(fare_rwf) AS fare_rwf FROM ridehail.trips ORDER BY event_ts DESC LIMIT 8;" 2>nul

echo.
echo  [2] NEWEST LATE-TRIP ALERTS  ^(flagged by our consumer group^)
echo      note the Kafka partition and offset - business logic tied to stream position
echo  -------------------------------------------------------------------------------
mysql --skip-ssl -u root -e "SELECT driver_id AS driver, CONCAT(pickup_zone,' > ',dropoff_zone) AS route, distance_km AS km, ROUND(duration_min,1) AS took, ROUND(duration_min/(distance_km/30*60),2) AS over_x, partition_id AS part, kafka_offset AS offset FROM ridehail.trip_alerts ORDER BY detected_at DESC LIMIT 6;" 2>nul

echo.
echo  [3] MODEL PREDICTIONS vs ACTUAL  ^(deployed MLlib GBT, random sample^)
echo  -------------------------------------------------------------------------------
mysql --skip-ssl -u root -e "SELECT CONCAT(pickup_zone,' > ',dropoff_zone) AS route, distance_km AS km, traffic_level AS traffic, actual_min AS actual, predicted_min AS predicted, error_min AS error FROM ridehail.predictions ORDER BY RAND() LIMIT 6;" 2>nul
mysql --skip-ssl -u root -e "SELECT COUNT(*) AS scored, ROUND(AVG(ABS(error_min)),3) AS mae_min, ROUND(MAX(ABS(error_min)),2) AS worst_min, ROUND(SUM(CASE WHEN ABS(error_min)^<=3 THEN 1 ELSE 0 END)*100.0/COUNT(*),1) AS pct_within_3min FROM ridehail.predictions;" 2>nul
echo      MAE near 2.2 min against a generator noise floor of 1.8 min.
echo      A model scoring BELOW that floor would indicate leakage, not skill.

echo.
echo ===============================================================================
echo   Refreshing every %INTERVAL%s. Ctrl+C to stop.
echo ===============================================================================

timeout /t %INTERVAL% /nobreak >nul
goto loop