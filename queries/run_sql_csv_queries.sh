#!/usr/bin/env bash

rm -f -r output/sql/csv &&
mkdir -p output/sql/csv &&

echo -n "" > output/sql/csv/times.txt &&

mkdir -p output/sql/csv/hdfs &&

for pyfiledir in sql/csv/*.py;
do
    pyfile="$(basename -- "$pyfiledir")"
    query="${pyfile%%_*}"

    spark-submit "$pyfiledir"
    hadoop fs -get hdfs://master:9000/output/"$query".csv output/sql/csv/hdfs/"$query"
done

# spark-submit sql/csv/q1_sql_csv.py &&
# hadoop fs -get hdfs://master:9000/output/q1.csv output/sql/csv/hdfs/q1 &&

hadoop fs -rm -r hdfs://master:9000/output/. &&

for dir in output/sql/csv/hdfs/*/;
do
    for file in "$dir"/*.csv;
    do
        mv "$file" output/sql/csv/"$(basename "$dir")".csv
    done
done

rm -r output/sql/csv/hdfs
