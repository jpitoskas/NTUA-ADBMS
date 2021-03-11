#!/usr/bin/env bash

mkdir -p output/rdd &&
mkdir -p output/sql/csv &&
mkdir -p output/sql/parquet &&

echo -n "" > output/rdd/times.txt &&
# echo -n "" > output/sql/csv/times.txt &&
# echo -n "" > output/sql/parquet/times.txt &&

spark-submit rdd/q1_rdd.py &&
spark-submit rdd/q2_rdd.py &&
spark-submit rdd/q3_rdd.py &&
spark-submit rdd/q4_rdd.py
