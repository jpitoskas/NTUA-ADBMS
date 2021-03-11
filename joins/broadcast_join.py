from pyspark.sql import SparkSession
from io import StringIO
import csv

def split_complex(x):
    return list(csv.reader(StringIO(x), delimiter=','))[0]

def map_genres(x):
    movie_info = split_complex(x)

    movie_id = int(movie_info[0])
    genre = movie_info[1]

    return (movie_id, genre)

def map_ratings(x):
    user_info = split_complex(x)

    user_id = int(user_info[0])
    movie_id = int(user_info[1])
    rating = float(user_info[2])

    return (movie_id, (user_id, rating))

spark = SparkSession.builder.appName("Broadcast-Join").getOrCreate()
sc = spark.sparkContext

genres = sc.textFile("hdfs://master:9000/movies/movie_genres.csv")

sc.broadcast(genres.collect())

ratings = sc.textFile("hdfs://master:9000/movies/ratings.csv")
