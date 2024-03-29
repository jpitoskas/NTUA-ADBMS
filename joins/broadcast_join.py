from pyspark.sql import SparkSession
from io import StringIO
import csv
import time

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

def myJoin(rating):
    movie_id = rating[0]
    rate_info = rating[1]

    if movie_id in broadcast_genres.keys():
        broadcast_genres[movie_id].data.append(rate_info)

        return [(movie_id, tuple(broadcast_genres[movie_id].data))]
    else:
        return []

spark = SparkSession.builder.appName("Broadcast-Join").getOrCreate()
sc = spark.sparkContext

start_time = time.time()

genres = sc.textFile("hdfs://master:9000/movies/movie_genres_100.csv"). \
        map(map_genres)

broadcast_genres = sc.broadcast(genres.groupByKey().collectAsMap()).value

ratings = sc.textFile("hdfs://master:9000/movies/ratings.csv"). \
        map(map_ratings).cache()


joined = ratings. \
        flatMap(myJoin)
print(joined.count())

elapsed_time = (time.time() - start_time)
print("\n--- %s seconds ---\n" % elapsed_time)

f = open("output/join_times.txt", "a")
f.write("Broadcast:"+str(elapsed_time)+"\n")
f.close()