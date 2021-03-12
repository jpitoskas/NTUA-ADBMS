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

    movie_id = int(user_info[1])
    rating = float(user_info[2])

    return (movie_id, (rating, 1))

start_time = time.time()

spark = SparkSession.builder.appName("Q3-RDD").getOrCreate()
sc = spark.sparkContext


genres = sc.textFile("hdfs://master:9000/movies/movie_genres.csv"). \
    map(map_genres). \
    cache()


ratings = sc.textFile("hdfs://master:9000/movies/ratings.csv"). \
    map(map_ratings). \
    reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1])). \
    map(lambda x: (x[0], x[1][0]/x[1][1])). \
    join(genres). \
    map(lambda x: (x[1][1], (x[1][0], 1))). \
    reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1])). \
    map(lambda x: (x[0], x[1][0]/x[1][1], x[1][1]))

f = open("output/rdd/q3.txt", "a")
for i in ratings.collect():
    print(i)
    f.write(str(i)+"\n")
f.close()

# print(genres.count())
# print(ratings.count())

elapsed_time = (time.time() - start_time)
print("\n--- %s seconds ---\n" % elapsed_time)

f = open("output/rdd/times.txt", "a")
f.write("Q3:"+str(elapsed_time)+"\n")
f.close()