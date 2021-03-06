from pyspark.sql import SparkSession
import datetime as dt
from io import StringIO
import csv
import time

def split_complex(x):
    return list(csv.reader(StringIO(x), delimiter=','))[0]


def filter2000(x):
    movie_info = split_complex(x)
    desc = movie_info[2]
    date = movie_info[3]

    return (desc and date and dt.datetime.strptime(date.split("T")[0], "%Y-%m-%d").year >= 2000)

def five_years(x):
    year = x[1][0][0]
    if year < 2005:
        res = "2000-2004"
    elif year < 2010:
        res = "2005-2009"
    elif year < 2015:
        res = "2010-2014"
    else:
        res = "2015-2019"
    
    return (res, (x[1][0][1], 1))

def map_genres(x):
    movie_info = split_complex(x)

    movie_id = int(movie_info[0])
    genre = movie_info[1]

    return (movie_id, genre)

def map_movies(x):
    movie_info = split_complex(x)

    movie_id = int(movie_info[0])
    description = len(movie_info[2].split(" "))
    date = dt.datetime.strptime(movie_info[3].split("T")[0], "%Y-%m-%d")

    return (movie_id, (date.year, description))

start_time = time.time()

spark = SparkSession.builder.appName("Q4-RDD").getOrCreate()
sc = spark.sparkContext


genres = sc.textFile("hdfs://master:9000/movies/movie_genres.csv"). \
    map(map_genres). \
    filter(lambda x: x[1] == 'Drama')
    # join(ratings). \
    # map(lambda x: (x[1][1], (x[1][0], 1)))
    # reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1])). \
    # map(lambda x: (x[0], x[1][0]/x[1][1]))
    # filter(lambda x: x[1] > 3.0)


movies = sc.textFile("hdfs://master:9000/movies/movies.csv"). \
    filter(filter2000). \
    map(map_movies). \
    join(genres). \
    map(five_years). \
    reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1])). \
    map(lambda x: (x[0], x[1][0]/x[1][1])). \
    sortBy(lambda x: x[0], ascending=True)


for i in movies.take(10):
    print(i)

elapsed_time = (time.time() - start_time)
print("\n--- %s seconds ---\n" % elapsed_time)