@echo off
rem ===================================================================
rem  watch_growth.cmd - the demo's single best window.
rem
rem  Refreshes every 15 seconds showing all four layers of the pipeline
rem  moving together:
rem     Kafka offsets   - records published to the topic
rem     MariaDB rows    - records the JDBC sink has written
rem     HDFS size/files - records the HDFS3 sink has written as Parquet
rem     alerts          - records the consumer group flagged as late
rem
rem  WHY THIS MATTERS IN THE DEFENCE:
rem  Four independent numbers climbing at once is proof the whole chain
rem  is live. Change RATE in the generator window and watch them all
rem  accelerate - that is velocity demonstrated rather than claimed.
rem
rem  RUN IN A FRESH WINDOW. Hadoop commands need Java 8, so do NOT run
rem  this in a window where JAVA_HOME was set to Java 17 for Connect.
rem
rem  Ctrl+C to stop.
rem ===================================================================
setlocal enabledelayedexpansion
title PIPELINE GROWTH MONITOR

set KAFKA=C:\kafka
set INTERVAL=15

rem Remember the previous reading so we can show the delta - the delta is
rem what makes it obvious the pipeline is live rather than just populated.
set PREV_TRIPS=0
set PREV_KAFKA=0

:loop
cls
echo ===============================================================================
echo   RWANDARIDE - LIVE PIPELINE GROWTH            %date%  %time:~0,8%
echo ===============================================================================
echo.

rem ---------------------------------------------------------------- Kafka
echo  [1] KAFKA  topic trip-events
set TOTAL_KAFKA=0
for /f "tokens=2,3 delims=:" %%a in ('%KAFKA%\bin\windows\kafka-get-offsets.bat --bootstrap-server localhost:9092 --topic trip-events 2^>nul') do (
  echo        partition %%a  ...  offset %%b
  set /a TOTAL_KAFKA=!TOTAL_KAFKA!+%%b
)
set /a KAFKA_DELTA=!TOTAL_KAFKA!-!PREV_KAFKA!
if !PREV_KAFKA! EQU 0 set KAFKA_DELTA=0
echo        ----------------------------------------
echo        TOTAL PUBLISHED  : !TOTAL_KAFKA!    ^(+!KAFKA_DELTA! since last refresh^)
echo.

rem ---------------------------------------------------------------- MariaDB
echo  [2] MARIADB  operational store  ^(written by the Kafka Connect JDBC sink^)
for /f "skip=1 tokens=*" %%a in ('mysql --skip-ssl -u root -N -B -e "SELECT COUNT(*) FROM ridehail.trips;" 2^>nul') do set TRIPS=%%a
for /f "tokens=*" %%a in ('mysql --skip-ssl -u root -N -B -e "SELECT COUNT(*) FROM ridehail.trips;" 2^>nul') do set TRIPS=%%a
if "!TRIPS!"=="" set TRIPS=0
set /a TRIPS_DELTA=!TRIPS!-!PREV_TRIPS!
if !PREV_TRIPS! EQU 0 set TRIPS_DELTA=0
echo        trips            : !TRIPS!    ^(+!TRIPS_DELTA! since last refresh^)

for /f "tokens=*" %%a in ('mysql --skip-ssl -u root -N -B -e "SELECT COUNT(*) FROM ridehail.trip_alerts;" 2^>nul') do set ALERTS=%%a
if "!ALERTS!"=="" set ALERTS=0
echo        late-trip alerts : !ALERTS!    ^(written by the consumer group^)
echo.

rem ---------------------------------------------------------------- HDFS
echo  [3] HDFS  historical store  ^(written by the Kafka Connect HDFS3 sink^)
for /f "tokens=1,2" %%a in ('hdfs dfs -du -s -h /data/trip-events 2^>nul') do (
  echo        parquet size     : %%a %%b
)
for /f "tokens=2" %%a in ('hdfs dfs -count /data/trip-events 2^>nul') do set NFILES=%%a
echo        parquet files    : !NFILES!
echo.
echo        newest file ^(should be within the last ~60 seconds^):
for /f "tokens=*" %%a in ('hdfs dfs -ls -t -R /data/trip-events 2^>nul ^| findstr /i ".parquet"') do (
  echo          %%a
  goto :gotfile
)
echo          ^(none found - is the HDFS sink running?^)
:gotfile
echo.

echo ===============================================================================
echo   Four independent numbers climbing together = the whole chain is live.
echo   Change RATE in the generator window and watch them all accelerate.
echo   Refreshing every %INTERVAL%s ... Ctrl+C to stop.
echo ===============================================================================

set PREV_TRIPS=!TRIPS!
set PREV_KAFKA=!TOTAL_KAFKA!

timeout /t %INTERVAL% /nobreak >nul
goto loop
