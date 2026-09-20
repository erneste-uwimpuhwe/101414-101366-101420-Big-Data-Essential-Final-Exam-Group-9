@echo off
rem ===================================================================
rem  start_all.cmd - opens every pipeline window, in the right order,
rem  each with the right JAVA_HOME.
rem
rem  ORDER IS NOT ARBITRARY:
rem    MariaDB must be up before Kafka Connect, or the JDBC sink fails
rem    validation and takes the whole Connect worker down with it. That
rem    cost us twelve hours of lost HDFS ingestion once already.
rem
rem  THE TWO-JAVA PROBLEM:
rem    Hadoop rejects Java 17. Kafka Connect rejects Java 8, because the
rem    HDFS3 connector bundles Spring classes compiled for 17. So the
rem    Connect window gets JAVA_HOME overridden and nothing else does.
rem
rem  Run from a FRESH window (system JAVA_HOME = Java 8):
rem      cd /d D:\bigdata-final\scripts
rem      start_all.cmd
rem ===================================================================
setlocal

echo.
echo ===============================================================
echo   RWANDARIDE - STARTING THE PIPELINE
echo ===============================================================
echo.

rem ---------------------------------------------------- 0. MariaDB gate
echo [0/8] Checking MariaDB...
mysql --skip-ssl -u root -e "SELECT 1;" >nul 2>&1
if errorlevel 1 (
  echo.
  echo   ** MariaDB is DOWN. Nothing else can start safely. **
  echo.
  echo   Open an ADMINISTRATOR cmd window and run:
  echo       net start MariaDB
  echo.
  echo   Then run this script again.
  pause
  exit /b 1
)
echo       MariaDB is up.

rem ---------------------------------------------------- 1. HDFS
echo [1/8] Starting HDFS ^(NameNode + DataNode^)...
start "1 - HDFS" cmd /k "start-dfs.cmd"
timeout /t 25 /nobreak >nul

rem ---------------------------------------------------- 2. Kafka broker
echo [2/8] Starting Kafka broker...
start "2 - KAFKA BROKER" cmd /k "cd /d C:\kafka && .\bin\windows\kafka-server-start.bat .\config\kraft\server.properties"
timeout /t 30 /nobreak >nul

rem ---------------------------------------------------- 3. Kafka Connect
rem A random log filename each run, because Windows keeps a handle on the
rem previous one and refuses to reopen it.
echo [3/8] Starting Kafka Connect ^(Java 17^)...
start "3 - KAFKA CONNECT" cmd /k "set JAVA_HOME=C:\Program Files\Eclipse Adoptium\jdk-17.0.20.8-hotspot&& set HADOOP_HOME=C:\hadoop&& set PATH=C:\hadoop\bin;%PATH%&& cd /d C:\kafka&& .\bin\windows\connect-standalone.bat D:\bigdata-final\connect\connect-standalone.properties D:\bigdata-final\connect\mariadb-sink.properties D:\bigdata-final\connect\hdfs3-sink.properties > D:\connect_%RANDOM%.log 2>&1"
timeout /t 20 /nobreak >nul

rem ---------------------------------------------------- 4. producer
echo [4/8] Starting DRF producer on :8001...
start "4 - PRODUCER 8001" cmd /k "cd /d D:\bigdata-final\producer && python producer_api.py runserver 8001"
timeout /t 8 /nobreak >nul

rem ---------------------------------------------------- 5. generator
echo [5/8] Starting generator ^(RATE=50^)...
start "5 - GENERATOR" cmd /k "cd /d D:\bigdata-final\generator && set RATE=50 && python generate.py"
timeout /t 3 /nobreak >nul

rem ---------------------------------------------------- 6. consumers
echo [6/8] Starting consumer group ^(two instances^)...
start "6 - CONSUMER 1" cmd /k "cd /d D:\bigdata-final\consumer && set INSTANCE=consumer-1 && python consumer_app.py"
timeout /t 6 /nobreak >nul
start "6 - CONSUMER 2" cmd /k "cd /d D:\bigdata-final\consumer && set INSTANCE=consumer-2 && python consumer_app.py"
timeout /t 3 /nobreak >nul

rem ---------------------------------------------------- 7. dashboards
echo [7/8] Starting dashboard on :8000 and admin on :8080...
start "7 - DASHBOARD 8000" cmd /k "cd /d D:\bigdata-final\dashboard && python dashboard.py runserver 8000"
timeout /t 4 /nobreak >nul
start "7 - ADMIN 8080" cmd /k "cd /d D:\bigdata-final\tripsite && python manage.py runserver 8080"
timeout /t 4 /nobreak >nul

rem ---------------------------------------------------- 8. the monitor
echo [8/8] Starting the growth monitor...
start "8 - GROWTH MONITOR" cmd /k "cd /d D:\bigdata-final\scripts && watch_growth.cmd"

echo.
echo ===============================================================
echo   All windows launched.
echo.
echo   Give Kafka Connect 90 seconds - its plugin scan is slow.
echo   Then check the GROWTH MONITOR window: all four numbers
echo   should be climbing.
echo.
echo   Browser tabs to open:
echo     http://localhost:8000          dispatch board
echo     http://localhost:8080/admin/   Django admin
echo     http://127.0.0.1:8001/api/trips/   DRF form
echo     http://localhost:9870          HDFS NameNode
echo ===============================================================
echo.
pause
