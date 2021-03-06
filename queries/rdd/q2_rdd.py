from pyspark.sql import SparkSession
from io import StringIO
import csv
import time

def split_complex(x):
    return list(csv.reader(StringIO(x), delimiter=','))[0]


def mapping(x):
    user_info = split_complex(x)

    user_id = int(user_info[0])
    rating = float(user_info[2])

    return (user_id, (rating, 1))

start_time = time.time()

spark = SparkSession.builder.appName("Q2-RDD").getOrCreate()
sc = spark.sparkContext

mean_user_ratings = sc.textFile("hdfs://master:9000/movies/ratings.csv"). \
    map(mapping). \
    reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1])). \
    map(lambda x: (x[0], x[1][0]/x[1][1]))
    # filter(lambda x: x[1] > 3.0)

cnt = 0
for i in mean_user_ratings.collect():
    if i[1] > 3.0:
        cnt += 1

res = 100*(cnt/len(mean_user_ratings.collect()))
print("{:.2f} %".format(round(res, 2)))

elapsed_time = (time.time() - start_time)
print("\n--- %s seconds ---\n" % elapsed_time)