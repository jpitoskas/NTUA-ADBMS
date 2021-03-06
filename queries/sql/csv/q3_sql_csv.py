from pyspark.sql import SparkSession
import time

start_time = time.time()

spark = SparkSession.builder.appName("Q3-SQL").getOrCreate()

genres = spark.read.format('csv'). \
    options(header='false', inferSchema='true'). \
    load("hdfs://master:9000/movies/movie_genres.csv")

ratings = spark.read.format('csv'). \
    options(header='false', inferSchema='true'). \
    load("hdfs://master:9000/movies/ratings.csv")

genres.registerTempTable("genres")
ratings.registerTempTable("ratings")
# spark.udf.register("get_year", get_year)

sqlString = \
    "SELECT g._c1 AS Genre, AVG(q1.AVGRating) AS AverageRating, COUNT(*) AS Count FROM ( " + \
        "SELECT r._c1 AS MovieID, AVG(r._c2) AS AVGRating " + \
        "FROM ratings AS r " + \
        "GROUP BY 1 ) AS q1 " + \
    "INNER JOIN genres AS g " + \
    "ON g._c0 = q1.MovieID " + \
    "GROUP BY g._c1 "

res = spark.sql(sqlString)
res.show()

elapsed_time = (time.time() - start_time)
print("\n--- %s seconds ---\n" % elapsed_time)