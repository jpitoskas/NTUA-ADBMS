#!/usr/bin/env bash

mkdir -p output &&

echo -n "" > output/join_times.txt &&

spark-submit broadcast_join.py &&
spark-submit repartition_join.py