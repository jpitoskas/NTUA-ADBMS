#!/usr/bin/env bash

shuf -n 100 movie_genres.csv > movie_genres_100.csv &&

hadoop fs -put movie_genres_100.csv hdfs://master:9000/movies/. &&

hadoop fs -ls hdfs://master:9000/movies/.