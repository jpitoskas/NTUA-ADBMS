from pyspark.sql import SparkSession
import datetime as dt
import time

def get_year(x):
    return x.year

start_time = time.time()

spark = SparkSession.builder.appName("Q1-SQL").getOrCreate()

movies = spark.read.format('csv'). \
    options(header='false', inferSchema='true'). \
    load("hdfs://master:9000/movies/movies.csv")

movies.registerTempTable("movies")
# spark.udf.register("get_year", get_year)

sqlString = \
    "SELECT YEAR(a._c3) AS Year, a._c1 AS Title, (100*(a._c6 - a._c5)/a._c5) AS Revenue " + \
    "FROM movies AS a " + \
    "INNER JOIN ( " + \
        "SELECT YEAR(_c3) AS Year, MAX(100*(_c6 -_c5)/_c5) AS Revenue " + \
        "FROM movies " + \
        "WHERE _c3 IS NOT NULL AND _c5 <> 0 AND _c6 <> 0 AND YEAR(_c3) >= 2000 " + \
        "GROUP BY YEAR(_c3) " + \
    ") AS b ON YEAR(a._c3) = b.Year AND (100*(a._c6 - a._c5)/a._c5) = b.Revenue " + \
    "ORDER BY 1 "

res = spark.sql(sqlString)
res.show()

elapsed_time = (time.time() - start_time)
print("\n--- %s seconds ---\n" % elapsed_time)