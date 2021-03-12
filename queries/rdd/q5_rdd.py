from pyspark.sql import SparkSession
from io import StringIO
import csv
import time

def split_complex(x):
    return list(csv.reader(StringIO(x), delimiter=','))[0]


def minMax(a,b):
    if a[1] != b[1]:
        return min(a,b, key=lambda item:item[1])
    else:
        return max(a,b, key=lambda item:item[2])

def map_movies(x):
    movie_info = split_complex(x)

    movie_id = int(movie_info[0])
    title = movie_info[1]
    popularity = float(movie_info[7])

    return (movie_id, (title, popularity))

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

start_time = time.time()

spark1 = SparkSession.builder.appName("Q5-RDD-1").getOrCreate()
sc1 = spark1.sparkContext

genres = sc1.textFile("hdfs://master:9000/movies/movie_genres.csv"). \
    map(map_genres). \
    cache()
    # filter(lambda x: x[1] == 'Action'). \

movies = sc1.textFile("hdfs://master:9000/movies/movies.csv"). \
    map(map_movies). \
    join(genres). \
    cache()

ratings = sc1.textFile("hdfs://master:9000/movies/ratings.csv"). \
    map(map_ratings). \
    join(movies). \
    map(lambda x: ( (x[1][1][1], x[1][0][0]), ((x[1][1][0][0], x[1][0][1], x[1][1][0][1]), (x[1][1][0][0], x[1][0][1], x[1][1][0][1]), 1))). \
    reduceByKey(lambda x, y: ((max(x[0], y[0], key=lambda item:(item[1], item[2]))), (minMax(x[1], y[1])), x[2]+y[2])). \
    map(lambda x: ( x[0][0], (x[0][1], x[1]))) . \
    reduceByKey(lambda x, y: max(x, y, key=lambda item:item[1][2])). \
    map(lambda x: ( x[0], x[1][0], x[1][1][2], x[1][1][0][0], x[1][1][0][1], x[1][1][1][0], x[1][1][1][1])). \
    sortBy(lambda x: x[0], ascending=True). \
    cache()

f = open("output/rdd/q5.txt", "a")
for i in ratings.collect():
    print(i)
    f.write(str(i)+"\n")
f.close()

# print(ratings.count())

elapsed_time = (time.time() - start_time)
print("\n--- %s seconds ---\n" % elapsed_time)

f = open("output/rdd/times.txt", "a")
f.write("Q5:"+str(elapsed_time)+"\n")
f.close()