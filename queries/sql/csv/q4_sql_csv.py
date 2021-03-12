from pyspark.sql import SparkSession
import time

def five_years(year):
    if year < 2005:
        res = "2000-2004"
    elif year < 2010:
        res = "2005-2009"
    elif year < 2015:
        res = "2010-2014"
    else:
        res = "2015-2019"
    
    return res

def description_len(desc):
    return len(desc.split(" "))


start_time = time.time()

spark = SparkSession.builder.appName("Q4-SQL").getOrCreate()

genres = spark.read.format('csv'). \
    options(header='false', inferSchema='true'). \
    load("hdfs://master:9000/movies/movie_genres.csv")

movies = spark.read.format('csv'). \
    options(header='false', inferSchema='true'). \
    load("hdfs://master:9000/movies/movies.csv")

genres.registerTempTable("genres")
movies.registerTempTable("movies")
spark.udf.register("five_years", five_years)
spark.udf.register("description_len", description_len)

sqlString = \
    "SELECT five_years(YEAR(m._c3)) AS 5YearPeriod, AVG(description_len(m._c2)) AS DescriptionLength " + \
    "FROM genres AS g " + \
    "INNER JOIN movies AS m " + \
    "ON g._c0 = m._c0 " + \
    "WHERE g._c1 = 'Drama' AND m._c3 IS NOT NULL AND m._c2 IS NOT NULL AND YEAR(m._c3) >= 2000 " + \
    "GROUP BY 1 " + \
    "ORDER BY 1 ASC "

res = spark.sql(sqlString)
res.show()
res.coalesce(1).write.csv("hdfs://master:9000/output/q4.csv")

elapsed_time = (time.time() - start_time)
print("\n--- %s seconds ---\n" % elapsed_time)

f = open("output/sql/csv/times.txt", "a")
f.write("Q4:"+str(elapsed_time)+"\n")
f.close()