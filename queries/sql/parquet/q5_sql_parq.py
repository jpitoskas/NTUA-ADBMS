from pyspark.sql import SparkSession
import time

# def five_years(year):
#     if year < 2005:
#         res = "2000-2004"
#     elif year < 2010:
#         res = "2005-2009"
#     elif year < 2015:
#         res = "2010-2014"
#     else:
#         res = "2015-2019"
    
#     return res

# def description_len(desc):
#     return len(desc.split(" "))


start_time = time.time()

spark = SparkSession.builder.appName("Q5-SQL").getOrCreate()

genres = spark.read.parquet("hdfs://master:9000/movies/movie_genres.parquet")

movies = spark.read.parquet("hdfs://master:9000/movies/movies.parquet")

ratings = spark.read.parquet("hdfs://master:9000/movies/ratings.parquet")

genres.registerTempTable("genres")
movies.registerTempTable("movies")
ratings.registerTempTable("ratings")
# spark.udf.register("five_years", five_years)
# spark.udf.register("description_len", description_len)

sqlString_ratings_and_genres = \
    "SELECT r._c0 AS UserID, g._c1 AS Genre, COUNT(r._c2) AS Count " + \
    "FROM genres AS g " + \
    "INNER JOIN ratings AS r " + \
    "ON g._c0 = r._c1 " + \
    "GROUP BY 1, 2 "

    # "SELECT five_years(YEAR(m._c3)) AS 5YearPeriod, AVG(description_len(m._c2)) AS DescriptionLength " + \
    # "FROM genres AS g " + \
    # "INNER JOIN movies AS m " + \
    # "ON g._c0 = m._c0 " + \
    # "WHERE g._c1 = 'Drama' AND m._c3 IS NOT NULL AND m._c2 IS NOT NULL AND YEAR(m._c3) >= 2000 " + \
    # "GROUP BY 1 " + \
    # "ORDER BY 1 ASC "

ratingsAndCounts = spark.sql(sqlString_ratings_and_genres)
ratingsAndCounts.createOrReplaceTempView("ratingsAndCounts")

sqlString_max_rating_counts = \
    "SELECT a.Genre, a.UserID, a.Count AS RatingCount " + \
    "FROM ratingsAndCounts AS a " + \
    "INNER JOIN ( " + \
        "SELECT Genre, MAX(Count) AS MaxCount " + \
        "FROM ratingsAndCounts " + \
        "GROUP BY 1 " + \
    ") AS b ON a.Genre = b.Genre AND a.Count = b.MaxCount " + \
    "WHERE a.Genre = 'Action' "

maxRatingCounts = spark.sql(sqlString_max_rating_counts)
maxRatingCounts.createOrReplaceTempView("maxRatingCounts")

sqlString_max_count_all_ratings = \
    "SELECT Genre, UserID, RatingCount, r._c1 AS MovieID, r._c2 AS MovieRating " + \
    "FROM maxRatingCounts as mrc " + \
    "INNER JOIN ratings as r " + \
    "ON mrc.UserID = r._c0 "

maxCountAllRatings = spark.sql(sqlString_max_count_all_ratings)
maxCountAllRatings.createOrReplaceTempView("maxCountAllRatings")

sqlString = \
    "SELECT a.Genre, a.UserID, a.RatingCount, m._c1 AS MovieTitle, a.MovieRating, m._c7 AS Popularity " + \
    "FROM maxCountAllRatings AS a " + \
    "INNER JOIN ( " + \
        "SELECT Genre, UserID, ΜΑΧ(MovieRating) AS MaxMovieRating " + \
        "FROM maxCountAllRatings " + \
        "GROUP BY 1, 2 " + \
    ") AS b ON a.Genre = b.Genre AND a.UserID = b.UserID AND a.MovieRating = b.MaxMovieRating " + \
    "INNER JOIN movies AS m " + \
    "ON a.MovieID = m._c0 "

res = spark.sql(sqlString)
res.show()

elapsed_time = (time.time() - start_time)
print("\n--- %s seconds ---\n" % elapsed_time)