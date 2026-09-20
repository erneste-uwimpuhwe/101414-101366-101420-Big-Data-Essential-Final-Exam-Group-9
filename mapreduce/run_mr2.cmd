@echo off
rem ===================================================================
rem  Hadoop Streaming MapReduce job: trips and pace by traffic level.
rem  The PySpark equivalent of this aggregation is Insight 2 in
rem  spark_jobs\insights.py - that pairing is the bonus comparison.
rem
rem  MUST run from an ADMINISTRATOR cmd window. YARN symlinks the mapper
rem  and reducer into each container's working directory, and creating a
rem  symlink on Windows needs elevated privilege - without it containers
rem  die with "CreateSymbolicLink error (1314)".
rem
rem  JAVA_HOME is forced to Java 8: Hadoop rejects Java 17, which the
rem  Kafka Connect windows set.
rem
rem  WHY -cacheFile AND NOT -files:
rem  -files takes one comma-separated list, but cmd splits the comma into
rem  a separate argument and Hadoop reports "Found 1 unexpected argument".
rem  Passing -files twice silently drops one script, and the reduce tasks
rem  then fail with "python: can't open file reducer.py". -cacheFile is a
rem  Streaming option parsed by StreamJob rather than by
rem  GenericOptionsParser, so it can be repeated, one file per flag, with
rem  no commas. The #alias names the file inside the container.
rem ===================================================================
setlocal

set JAVA_HOME=C:\Java\Jdk
set JAR=C:\hadoop\share\hadoop\tools\lib\hadoop-streaming-3.2.4.jar

echo Removing any previous output directory...
call hdfs dfs -rm -r -skipTrash /data/mr_traffic_output

echo.
echo ============================================================
echo  MAPREDUCE JOB START: %time%
echo ============================================================
echo.

call hadoop jar "%JAR%" -D mapreduce.job.name=trip-traffic-stats-mapreduce -D mapreduce.job.reduces=1 -cacheFile hdfs:///mr/mapper.py#mapper.py -cacheFile hdfs:///mr/reducer.py#reducer.py -mapper "python mapper.py" -reducer "python reducer.py" -input /data/trips_text -output /data/mr_traffic_output

echo.
echo ============================================================
echo  MAPREDUCE JOB END:   %time%
echo ============================================================
echo.
echo --- RESULT: traffic_level / trip_count / avg_duration / avg_min_per_km ---
echo.
call hdfs dfs -cat /data/mr_traffic_output/part-00000
echo.
pause