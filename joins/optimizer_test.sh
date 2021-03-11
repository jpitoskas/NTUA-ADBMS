#!/usr/bin/env bash

mkdir -p output &&

echo -n "" > output/optimizer_times.txt &&

spark-submit query_optimizer.py N &&
spark-submit query_optimizer.py Y