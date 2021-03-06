#!/usr/bin/env bash

hadoop fs -mkdir hdfs://master:9000/movies ||

hadoop fs -put movies.csv hdfs://master:9000/movies/. ||
hadoop fs -put movie_genres.csv hdfs://master:9000/movies/. ||
hadoop fs -put ratings.csv hdfs://master:9000/movies/. ||

spark-submit csv2parquet.py movies ||
spark-submit csv2parquet.py movie_genres ||
spark-submit csv2parquet.py ratings ||

hadoop fs -ls hdfs://master:9000/movies/.
