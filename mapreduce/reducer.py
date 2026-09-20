#!/usr/bin/env python
"""
Hadoop Streaming REDUCER.

Receives the mapper's output, already sorted by key, one line at a time.
Because it arrives sorted, all lines for one traffic level appear
consecutively - so we accumulate until the key changes, then emit.

That "watch for the key to change" loop is boilerplate every Streaming
reducer has to write by hand. Spark's groupBy().agg() does the equivalent
internally, which is the clearest single illustration of the difference in
code complexity between the two models.

Output:  traffic_level  trip_count  avg_duration  avg_min_per_km
"""
import sys


def emit(key, count, dur_sum, mpk_sum):
    if key is None or count == 0:
        return
    sys.stdout.write(
        f"{key}\t{count}\t{dur_sum / count:.2f}\t{mpk_sum / count:.3f}\n")


current = None
count = 0
dur_sum = 0.0
mpk_sum = 0.0

for line in sys.stdin:
    parts = line.rstrip("\n").split("\t")
    if len(parts) < 3:
        continue

    key = parts[0]
    try:
        duration = float(parts[1])
        distance = float(parts[2])
    except ValueError:
        continue

    # The key changed, so the previous group is complete. This only works
    # because Hadoop guarantees the input is sorted by key.
    if key != current:
        emit(current, count, dur_sum, mpk_sum)
        current = key
        count = 0
        dur_sum = 0.0
        mpk_sum = 0.0

    count += 1
    dur_sum += duration
    mpk_sum += duration / distance

emit(current, count, dur_sum, mpk_sum)     # the final group