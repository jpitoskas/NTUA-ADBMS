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

    return (movie_id, ("G", genre))

def map_ratings(x):
    user_info = split_complex(x)

    user_id = int(user_info[0])
    movie_id = int(user_info[1])
    rating = float(user_info[2])

    return (movie_id, ("R", user_id, rating))

def map_join(buffers):
    movie_id = buffers[0]
    ratings = buffers[1][0].data
    genres = buffers[1][1].data

    res = []

    if (len(ratings) != 0 and len(genres) != 0):
        for rating in ratings:
            for genre in genres:
                res.append((movie_id, rating, genre))
        return res
    else:
        return []


spark = SparkSession.builder.appName("Repartition-Join").getOrCreate()
sc = spark.sparkContext

start_time = time.time()

genres = sc.textFile("hdfs://master:9000/movies/movie_genres_100.csv"). \
        map(map_genres). \
        cache()

ratings = sc.textFile("hdfs://master:9000/movies/ratings.csv"). \
        map(map_ratings). \
        cache(). \
        cogroup(genres). \
        flatMap(map_join)
        # repartition(2). \

# for i in ratings.take(5):
#     print(i)

print(ratings.count())

elapsed_time = (time.time() - start_time)
print("\n--- %s seconds ---\n" % elapsed_time)

f = open("output/join_times.txt", "a")
f.write("Repartition:"+str(elapsed_time)+"\n")
f.close()