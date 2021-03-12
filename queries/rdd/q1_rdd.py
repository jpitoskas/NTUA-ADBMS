from pyspark.sql import SparkSession
import datetime as dt
from io import StringIO
import csv
import time

def split_complex(x):
    return list(csv.reader(StringIO(x), delimiter=','))[0]


def filtering(x):
    movie_info = split_complex(x)

    date = movie_info[3]
    cost = int(movie_info[5])
    income = int(movie_info[6])

    return (date and dt.datetime.strptime(date.split("T")[0], "%Y-%m-%d").year >= 2000 and cost !=0 and income != 0)

def mapping(x):
    movie_info = split_complex(x)

    movie_id = int(movie_info[0])
    title = movie_info[1]
    date = dt.datetime.strptime(movie_info[3].split("T")[0], "%Y-%m-%d")
    cost = int(movie_info[5])
    income = int(movie_info[6])

    if (income != 0 and cost != 0):
        revenue = 100*(income - cost)/cost

    return (date.year, (revenue, movie_id, title))

start_time = time.time()

spark = SparkSession.builder.appName("Q1-RDD").getOrCreate()
sc = spark.sparkContext

top_revenue_movies = sc.textFile("hdfs://master:9000/movies/movies.csv"). \
    filter(filtering). \
    map(mapping). \
    reduceByKey(lambda x, y: max(x, y, key=lambda item:item[0])). \
    map(lambda x: (x[0], x[1][2])). \
    sortBy(lambda x: x[0], ascending=True)
    # flatMap(lambda x : x.split(",")). \
    # map(lambda x : (x, 1))
    # reduceByKey(lambda x, y : x + y). \
    # sortBy(lambda x: x[1], ascending=False)

f = open("output/rdd/q1.txt", "a")
for i in top_revenue_movies.collect():
    print(i)
    f.write(str(i)+"\n")
f.close()

elapsed_time = (time.time() - start_time)
print("\n--- %s seconds ---\n" % elapsed_time)

f = open("output/rdd/times.txt", "a")
f.write("Q1:"+str(elapsed_time)+"\n")
f.close()