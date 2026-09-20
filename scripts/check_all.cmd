@echo off
rem ===================================================================
rem  check_all.cmd - one command that tells you whether every layer is
rem  alive. Run this before the demo, and any time something looks wrong.
rem
rem  RUN IN A FRESH WINDOW - hdfs needs Java 8, not the Java 17 that the
rem  Connect window uses.
rem ===================================================================
echo.
echo ===============================================================
echo   [1] JAVA PROCESSES - want NameNode, DataNode, Kafka, Connect
echo ===============================================================
jps

echo.
echo ===============================================================
echo   [2] MARIADB - every table should be non-zero
echo ===============================================================
mysql --skip-ssl -u root -e "SELECT 'trips' t, COUNT(*) n FROM ridehail.trips UNION SELECT 'alerts', COUNT(*) FROM ridehail.trip_alerts UNION SELECT 'zone_insight', COUNT(*) FROM ridehail.insight_zone_demand UNION SELECT 'hourly_insight', COUNT(*) FROM ridehail.insight_hourly_demand UNION SELECT 'traffic_insight', COUNT(*) FROM ridehail.insight_traffic_impact UNION SELECT 'predictions', COUNT(*) FROM ridehail.predictions UNION SELECT 'model_metrics', COUNT(*) FROM ridehail.model_metrics;"

echo.
echo ===============================================================
echo   [3] CONSUMER GROUPS - each needs a real CONSUMER-ID.
echo       "has no active members" means that task is DEAD.
echo ===============================================================
cd /d C:\kafka
echo --- trip-workers (our consumer app) ---
call .\bin\windows\kafka-consumer-groups.bat --bootstrap-server localhost:9092 --describe --group trip-workers
echo.
echo --- connect-mariadb-trips-sink ---
call .\bin\windows\kafka-consumer-groups.bat --bootstrap-server localhost:9092 --describe --group connect-mariadb-trips-sink
echo.
echo --- connect-hdfs3-trips-sink ---
call .\bin\windows\kafka-consumer-groups.bat --bootstrap-server localhost:9092 --describe --group connect-hdfs3-trips-sink

echo.
echo ===============================================================
echo   [4] HDFS - the newest file must be within the last ~60 seconds
echo ===============================================================
hdfs dfs -du -s -h /data/trip-events
hdfs dfs -ls -t /data/trip-events

echo.
echo ===============================================================
echo   If a sink says "no active members": restart Kafka Connect.
echo   If HDFS is stale but MariaDB is current: the HDFS task died.
echo ===============================================================
pause
