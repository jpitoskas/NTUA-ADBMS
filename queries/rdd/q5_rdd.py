from pyspark.sql import SparkSession
from io import StringIO
import csv
import time

def split_complex(x):
    return list(csv.reader(StringIO(x), delimiter=','))[0]


# def filter2000(x):
#     movie_info = split_complex(x)
#     desc = movie_info[2]
#     date = movie_info[3]

#     return (desc and date and dt.datetime.strptime(date.split("T")[0], "%Y-%m-%d").year >= 2000)

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

    return (movie_id, (user_id, rating, 1))

start_time = time.time()

spark1 = SparkSession.builder.appName("Q4-RDD-1").getOrCreate()
sc1 = spark1.sparkContext

spark2 = SparkSession.builder.appName("Q4-RDD-2").getOrCreate()
sc2 = spark2.sparkContext

genres = sc1.textFile("hdfs://master:9000/movies/movie_genres.csv"). \
    map(map_genres). \
    filter(lambda x: x[1] == 'Action')
    # join(ratings). \
    # map(lambda x: (x[1][1], (x[1][0], 1)))
    # reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1])). \
    # map(lambda x: (x[0], x[1][0]/x[1][1]))
    # filter(lambda x: x[1] > 3.0)

# movies = sc.textFile("hdfs://master:9000/movies/movies.csv"). \
#     map(map_movies). \
#     join(genres)
    # map(five_years). \
    # reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1])). \
    # map(lambda x: (x[0], x[1][0]/x[1][1])). \
    # sortBy(lambda x: x[0], ascending=True)

ratings = sc2.textFile("hdfs://master:9000/movies/ratings.csv"). \
    map(map_ratings). \
    join(genres)
    # join(movies)


# for i in movies.take(10):
#     print(i)

for i in ratings.take(10):
    print(i)

# print(genres.count())
# print(ratings.count())

elapsed_time = (time.time() - start_time)
print("\n--- %s seconds ---\n" % elapsed_time)