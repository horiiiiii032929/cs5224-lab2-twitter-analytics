"""Lecture 07: distributed distinct -> map -> shuffle/reduce -> shared storage.

One directed edge (source, target) contributes one followee to source and one
follower to target. Self-edges are retained: the specification does not exclude
these. Invalid/blank lines are counted; malformed nonblank rows fail the job.
"""
import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone
from pyspark import StorageLevel
from pyspark.sql import SparkSession


def parse_line(line):
    parts = line.split()
    if not parts:
        return ("blank", None)
    if len(parts) != 2 or not all(re.fullmatch(r"[0-9]+", p) for p in parts):
        return ("invalid", None)
    # Keep IDs as strings: identifiers are not quantities.
    return ("valid", (parts[0], parts[1]))


def contributions(edge):
    source, target = edge
    return [(source, (0, 1)), (target, (1, 0))]


def add_counts(a, b):
    return (a[0] + b[0], a[1] + b[1])


def count_users(edges, partitions):
    return edges.flatMap(contributions).reduceByKey(add_counts, numPartitions=partitions)


def analyze(spark, input_path, output_path, partitions=4):
    sc = spark.sparkContext
    parsed = sc.textFile(input_path, minPartitions=partitions).map(parse_line).persist(StorageLevel.MEMORY_AND_DISK)
    categories = dict(parsed.map(lambda row: (row[0], 1)).reduceByKey(lambda a, b: a + b).collect())
    if categories.get("invalid", 0):
        raise ValueError(f"Input contains {categories['invalid']} malformed rows; fix the input before running.")
    if not categories.get("valid", 0):
        raise ValueError("Input contains no valid directed edges.")
    edges = parsed.filter(lambda row: row[0] == "valid").map(lambda row: row[1]).distinct(partitions).persist(StorageLevel.MEMORY_AND_DISK)
    unique_edges = edges.count()
    self_edges = edges.filter(lambda pair: pair[0] == pair[1]).count()
    counts = count_users(edges, partitions).persist(StorageLevel.MEMORY_AND_DISK)
    users, followers, followees = counts.map(lambda row: (1, row[1][0], row[1][1])).fold((0, 0, 0), lambda a, b: tuple(x + y for x, y in zip(a, b)))
    if followers != unique_edges or followees != unique_edges:
        raise AssertionError("Degree sum invariant failed")
    required = dict(counts.filter(lambda row: row[0] in ("214328887", "107830991")).collect())
    metadata = {
        "input": input_path,
        "createdAt": datetime.now(timezone.utc).isoformat(),
        "sparkVersion": spark.version,
        "sparkMaster": sc.master,
        "partitions": counts.getNumPartitions(),
        "totalLines": sum(categories.values()),
        "validRows": categories.get("valid", 0),
        "blankRows": categories.get("blank", 0),
        "invalidRows": categories.get("invalid", 0),
        "duplicateRows": categories.get("valid", 0) - unique_edges,
        "uniqueEdges": unique_edges,
        "selfEdges": self_edges,
        "selfEdgePolicy": "retain",
        "users": users,
        "totalFollowers": followers,
        "totalFollowees": followees,
        "requiredUsers": {u: {"followers": c[0], "followees": c[1]} for u, c in required.items()},
    }
    # Distributed part files: never collect the complete dataset on the driver.
    counts.map(lambda row: json.dumps({"userId": row[0], "followers": row[1][0], "followees": row[1][1]})).saveAsTextFile(output_path.rstrip("/") + "/users")
    # Publish metadata last. Backend also checks both _SUCCESS markers.
    sc.parallelize([json.dumps(metadata)], 1).saveAsTextFile(output_path.rstrip("/") + "/metadata")
    print("LAB2_RESULT " + json.dumps(metadata), flush=True)
    counts.unpersist(); edges.unpersist(); parsed.unpersist()
    return metadata


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True, help="New directory/prefix; existing output is never overwritten")
    parser.add_argument("--master", help="Only set for local execution; EMR uses spark-submit --master yarn")
    parser.add_argument("--partitions", type=int, default=4)
    args = parser.parse_args()
    if args.partitions < 2:
        parser.error("Use at least two partitions to demonstrate parallel processing")
    if args.master:
        os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    builder = SparkSession.builder.appName("CS5224-Lab2-TwitterCounts")
    if args.master:
        builder = builder.master(args.master)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        analyze(spark, args.input, args.output, args.partitions)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
