#!/usr/bin/env bash

rm -f -r output/rdd &&
mkdir -p output/rdd &&

echo -n "" > output/rdd/times.txt &&

spark-submit rdd/q1_rdd.py &&
spark-submit rdd/q2_rdd.py &&
spark-submit rdd/q3_rdd.py &&
spark-submit rdd/q4_rdd.py &&
spark-submit rdd/q5_rdd.py
