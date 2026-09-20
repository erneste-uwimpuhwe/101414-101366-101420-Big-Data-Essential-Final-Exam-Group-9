#!/usr/bin/env python
"""
Hadoop Streaming MAPPER.

Reads one tab-delimited line at a time from stdin and emits
    traffic_level \t duration_min
to stdout. Hadoop then sorts and groups those lines by key before handing
them to the reducer.

Note what the mapper CANNOT do: it has no idea what the whole dataset looks
like, cannot see other records, and cannot hold state across the job. That
constraint is the entire MapReduce programming model - and the reason a
group-by-and-average needs two separate programs plus a shuffle phase,
where PySpark expresses the same thing in one line.

Input field positions (from the exported TSV):
    0 trip_id   1 driver_id   2 pickup_zone   3 dropoff_zone
    4 distance_km   5 traffic_level   6 weather   7 duration_min
"""
import sys

TRAFFIC = 5
DURATION = 7
DISTANCE = 4

for line in sys.stdin:
    fields = line.rstrip("\n").split("\t")

    # Defensive: a short or malformed line would raise IndexError and kill
    # the whole task. Streaming gives us no error tolerance mechanism, so
    # every mapper has to guard its own input by hand.
    if len(fields) < 8:
        sys.stderr.write("reporter:counter:TripStats,MalformedLines,1\n")
        continue

    try:
        duration = float(fields[DURATION])
        distance = float(fields[DISTANCE])
    except ValueError:
        sys.stderr.write("reporter:counter:TripStats,UnparseableNumbers,1\n")
        continue

    if distance <= 0 or duration <= 0:
        sys.stderr.write("reporter:counter:TripStats,RejectedRows,1\n")
        continue

    traffic = fields[TRAFFIC]
    # emit: key TAB value. Hadoop splits on the first tab.
    sys.stdout.write(f"{traffic}\t{duration}\t{distance}\n")