from pyspark.sql import SparkSession
import time

start_time = time.time()

spark = SparkSession.builder.appName("Q3-SQL").getOrCreate()

genres = spark.read.parquet("hdfs://master:9000/movies/movie_genres.parquet")

ratings = spark.read.parquet("hdfs://master:9000/movies/ratings.parquet")

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
res.coalesce(1).write.csv("hdfs://master:9000/output/q3.csv")

elapsed_time = (time.time() - start_time)
print("\n--- %s seconds ---\n" % elapsed_time)

f = open("output/sql/parquet/times.txt", "a")
f.write("Q3:"+str(elapsed_time)+"\n")
f.close()