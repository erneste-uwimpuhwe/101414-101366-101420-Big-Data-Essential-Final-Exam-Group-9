@echo off
rem ===================================================================
rem  watch_kafka.cmd - read the RAW records straight off the Kafka topic.
rem
rem  This is the most direct evidence available: the console consumer
rem  prints each message exactly as it sits in the log, including the
rem  partition it landed on and the schema envelope our DRF producer
rem  wraps around every record.
rem
rem  It joins NO consumer group of its own that matters, and starts from
rem  the END of the topic, so it shows only records arriving right now and
rem  does not disturb trip-workers or either Connect sink.
rem
rem  WHAT TO POINT AT IN THE DEMO:
rem    * Partition:N in the printed key line - different drivers, different
rem      partitions, same driver always the same one
rem    * the {"schema":...,"payload":...} envelope - this is what lets
rem      Kafka Connect create the MariaDB table and write Parquet with no
rem      hand-written DDL
rem
rem  Ctrl+C to stop.
rem ===================================================================
title RAW KAFKA MESSAGES - live off the topic

echo.
echo  Reading live messages off topic trip-events. Ctrl+C to stop.
echo  Each line shows the partition, the key (driver_id) and the payload.
echo.

cd /d C:\kafka
.\bin\windows\kafka-console-consumer.bat ^
  --bootstrap-server localhost:9092 ^
  --topic trip-events ^
  --property print.partition=true ^
  --property print.key=true ^
  --property print.offset=true ^
  --property key.separator=" | "
