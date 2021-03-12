from pyspark.sql import SparkSession
import datetime as dt
import time

start_time = time.time()

spark = SparkSession.builder.appName("Q2-SQL").getOrCreate()

ratings = spark.read.parquet("hdfs://master:9000/movies/ratings.parquet")

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
res.coalesce(1).write.csv("hdfs://master:9000/output/q2.csv")

elapsed_time = (time.time() - start_time)
print("\n--- %s seconds ---\n" % elapsed_time)

f = open("output/sql/parquet/times.txt", "a")
f.write("Q2:"+str(elapsed_time)+"\n")
f.close()