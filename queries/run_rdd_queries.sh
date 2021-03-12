#!/usr/bin/env bash

rm -f -r output/rdd &&
mkdir -p output/rdd &&

echo -n "" > output/rdd/times.txt &&

for pyfiledir in rdd/*.py;
do
    spark-submit "$pyfiledir"
done
