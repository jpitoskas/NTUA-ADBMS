from pyspark.sql import SparkSession
import time

start_time = time.time()

spark = SparkSession.builder.appName("Q2-SQL").getOrCreate()

ratings = spark.read.format('csv'). \
    options(header='false', inferSchema='true'). \
    load("hdfs://master:9000/movies/ratings.csv")

ratings.registerTempTable("ratings")
# spark.udf.register("get_year", get_year)

sqlString = \
    "SELECT 100*COUNT(*)/( " + \
        "SELECT COUNT(*) AS AllUserCNT FROM ( " + \
            "SELECT _c0 AS UserID, AVG(_c2) AS Rating " + \
            "FROM ratings " + \
            "GROUP BY _c0 ) " + \
    ") AS UserRatingPercentage FROM ( " + \
        "SELECT _c0 AS UserID, AVG(_c2) AS Rating " + \
        "FROM ratings " + \
        "GROUP BY _c0 ) AS r " + \
    "WHERE r.Rating > 3 "

res = spark.sql(sqlString)
res.show()

elapsed_time = (time.time() - start_time)
print("\n--- %s seconds ---\n" % elapsed_time)