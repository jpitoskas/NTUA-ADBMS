#!/usr/bin/env bash

rm -f -r output/sql/parquet &&
mkdir -p output/sql/parquet &&

echo -n "" > output/sql/parquet/times.txt &&

mkdir -p output/sql/parquet/hdfs &&

for pyfiledir in sql/parquet/*.py;
do
    pyfile="$(basename -- "$pyfiledir")"
    query="${pyfile%%_*}"

    spark-submit "$pyfiledir"
    hadoop fs -get hdfs://master:9000/output/"$query".csv output/sql/parquet/hdfs/"$query"
done

# spark-submit sql/csv/q1_sql_csv.py &&
# hadoop fs -get hdfs://master:9000/output/q1.csv output/sql/csv/hdfs/q1 &&

hadoop fs -rm -r hdfs://master:9000/output/. &&

for dir in output/sql/parquet/hdfs/*/;
do
    for file in "$dir"/*.csv;
    do
        mv "$file" output/sql/parquet/"$(basename "$dir")".csv
    done
done

rm -r output/sql/parquet/hdfs
