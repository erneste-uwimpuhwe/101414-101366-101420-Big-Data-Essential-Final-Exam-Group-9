@echo off
rem Runs the Hadoop Streaming job. Must run in a window where JAVA_HOME is
rem Java 8 (the system default) - NOT a window where it was set to 17 for
rem Kafka Connect.

setlocal
set STREAMING_JAR=%HADOOP_HOME%\share\hadoop\tools\lib\hadoop-streaming-3.2.4.jar
set MR_DIR=D:\bigdata-final\mapreduce

echo Removing any previous output directory...
call hdfs dfs -rm -r -skipTrash /data/mr_traffic_output 2>nul

echo.
echo Submitting the MapReduce job...
echo Start: %time%
echo.

call hadoop jar "%STREAMING_JAR%" ^
  -D mapreduce.job.name="trip-traffic-stats-mapreduce" ^
  -files file:///D:/bigdata-final/mapreduce/mapper.py#mapper.py,file:///D:/bigdata-final/mapreduce/reducer.py#reducer.py ^
  -mapper "python mapper.py" ^
  -reducer "python reducer.py" ^
  -input /data/trips_text ^
  -output /data/mr_traffic_output

echo.
echo End: %time%
echo.
echo --- result ---
call hdfs dfs -cat /data/mr_traffic_output/part-00000
pause
