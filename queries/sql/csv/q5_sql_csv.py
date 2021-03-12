from pyspark.sql import SparkSession
import time

start_time = time.time()

spark = SparkSession.builder.appName("Q5-SQL-1").getOrCreate()
# spark2 = SparkSession.builder.appName("Q5-SQL-2").getOrCreate()
# spark3 = SparkSession.builder.appName("Q5-SQL-3").getOrCreate()

genres = spark.read.format('csv'). \
    options(header='false', inferSchema='true'). \
    load("hdfs://master:9000/movies/movie_genres.csv")

movies = spark.read.format('csv'). \
    options(header='false', inferSchema='true'). \
    load("hdfs://master:9000/movies/movies.csv")

ratings = spark.read.format('csv'). \
    options(header='false', inferSchema='true'). \
    load("hdfs://master:9000/movies/ratings.csv")

genres.registerTempTable("genres")
movies.registerTempTable("movies")
ratings.registerTempTable("ratings")
# spark.udf.register("five_years", five_years)

sqlString_ratings_and_genres = \
    "SELECT r._c0 AS UserID, g._c1 AS Genre, r._c1 AS MovieID, r._c2 AS Rating " + \
    "FROM genres AS g " + \
    "INNER JOIN ratings AS r " + \
    "ON g._c0 = r._c1 "

ratingsAndGenres = spark.sql(sqlString_ratings_and_genres)
ratingsAndGenres.createOrReplaceTempView("ratingsAndGenres")
# ratingsAndGenres.show()

sqlString_ratings_and_genres_count = \
    "SELECT rg.UserID, rg.Genre, COUNT(rg.Rating) AS Count " + \
    "FROM ratingsAndGenres AS rg " + \
    "GROUP BY 1, 2 "

ratingsAndCounts = spark.sql(sqlString_ratings_and_genres_count)
ratingsAndCounts.createOrReplaceTempView("ratingsAndCounts")
# ratingsAndCounts.show()

sqlString_max_rating_counts = \
    "SELECT a.Genre AS MaxGenre, a.UserID AS MaxUserID, a.Count AS RatingCount " + \
    "FROM ratingsAndCounts AS a " + \
    "INNER JOIN ( " + \
        "SELECT Genre, MAX(Count) AS MaxCount " + \
        "FROM ratingsAndCounts " + \
        "GROUP BY 1 " + \
    ") AS b ON a.Genre = b.Genre AND a.Count = b.MaxCount "
    # "WHERE a.Genre = 'Action' "

maxRatingCounts = spark.sql(sqlString_max_rating_counts)
maxRatingCounts.createOrReplaceTempView("maxRatingCounts")
# maxRatingCounts.show()

sqlString_max_count_all_ratings = \
    "SELECT mrc.MaxGenre AS Genre, mrc.MaxUserID AS UserID, mrc.RatingCount, rg.MovieID, rg.Rating AS MovieRating " + \
    "FROM maxRatingCounts AS mrc " + \
    "INNER JOIN ratingsAndGenres AS rg " + \
    "ON mrc.MaxUserID = rg.UserID AND mrc.MaxGenre = rg.Genre "

maxCountAllRatings = spark.sql(sqlString_max_count_all_ratings)
maxCountAllRatings.createOrReplaceTempView("maxCountAllRatings")
# maxCountAllRatings.show()
# print(maxCountAllRatings.count())

sqlString_max_count_ratings = \
    "SELECT a.Genre AS GenreMax, a.UserID AS UserIDMax, a.RatingCount AS RatingCountMax, m._c1 AS MovieTitleMax, a.MovieRating AS MovieRatingMax, m._c7 AS PopularityMax " + \
    "FROM maxCountAllRatings AS a " + \
    "INNER JOIN ( " + \
        "SELECT Genre, UserID, MAX(MovieRating) AS MaxMovieRating " + \
        "FROM maxCountAllRatings " + \
        "GROUP BY 1, 2 " + \
    ") AS b ON a.Genre = b.Genre AND a.UserID = b.UserID AND a.MovieRating = b.MaxMovieRating " + \
    "INNER JOIN movies AS m " + \
    "ON a.MovieID = m._c0 "
    # "SELECT a.Genre AS GenreMax, a.UserID AS UserIDMax, a.RatingCount AS RatingCountMax, m._c1 AS MovieTitleMax, a.MovieRating AS MovieRatingMax, m._c7 AS PopularityMax " + \
    # "ORDER BY CAST(PopularityMax AS FLOAT) DESC "
    # "LIMIT 1 "

maxCountRatings = spark.sql(sqlString_max_count_ratings)
maxCountRatings.createOrReplaceTempView("maxCountRatings")
# maxCountRatings.show()

sqlString_max_count_ratings_popularity = \
    "SELECT a.GenreMax, a.UserIDMax, a.RatingCountMax, a.MovieTitleMax, a.MovieRatingMax, a.PopularityMax " + \
    "FROM maxCountRatings AS a " + \
    "INNER JOIN ( " + \
        "SELECT GenreMax, UserIDMax, MAX(CAST(PopularityMax AS FLOAT)) AS MaxPopularity " + \
        "FROM maxCountRatings " + \
        "GROUP BY 1, 2 " + \
    ") AS b ON a.GenreMax = b.GenreMax AND a.UserIDMax = b.UserIDMax AND a.PopularityMax = b.MaxPopularity "

maxCountRatingsPopularity = spark.sql(sqlString_max_count_ratings_popularity)
maxCountRatingsPopularity.createOrReplaceTempView("maxCountRatingsPopularity")
# maxCountRatingsPopularity.show()

sqlString_min_count_ratings = \
    "SELECT a.Genre AS GenreMin, a.UserID AS UserIDMin, a.RatingCount AS RatingCountMin, m._c1 AS MovieTitleMin, a.MovieRating AS MovieRatingMin, m._c7 AS PopularityMin " + \
    "FROM maxCountAllRatings AS a " + \
    "INNER JOIN ( " + \
        "SELECT Genre, UserID, MIN(MovieRating) AS MinMovieRating " + \
        "FROM maxCountAllRatings " + \
        "GROUP BY 1, 2 " + \
    ") AS b ON a.Genre = b.Genre AND a.UserID = b.UserID AND a.MovieRating = b.MinMovieRating " + \
    "INNER JOIN movies AS m " + \
    "ON a.MovieID = m._c0 "

minCountRatings = spark.sql(sqlString_min_count_ratings)
minCountRatings.createOrReplaceTempView("minCountRatings")
# minCountRatings.show()

sqlString_min_count_ratings_popularity = \
    "SELECT a.GenreMin, a.UserIDMin, a.RatingCountMin, a.MovieTitleMin, a.MovieRatingMin, a.PopularityMin " + \
    "FROM minCountRatings AS a " + \
    "INNER JOIN ( " + \
        "SELECT GenreMin, UserIDMin, MAX(CAST(PopularityMin AS FLOAT)) AS MaxPopularity " + \
        "FROM minCountRatings " + \
        "GROUP BY 1, 2 " + \
    ") AS b ON a.GenreMin = b.GenreMin AND a.UserIDMin = b.UserIDMin AND a.PopularityMin = b.MaxPopularity "

minCountRatingsPopularity = spark.sql(sqlString_min_count_ratings_popularity)
minCountRatingsPopularity.createOrReplaceTempView("minCountRatingsPopularity")
# minCountRatingsPopularity.show()

sqlString = \
    "SELECT maxcr.GenreMax, maxcr.UserIDMax, maxcr.RatingCountMax, maxcr.MovieTitleMax, " + \
    "maxcr.MovieRatingMax, mincr.MovieTitleMin, mincr.MovieRatingMin " + \
    "FROM maxCountRatingsPopularity AS maxcr " + \
    "INNER JOIN minCountRatingsPopularity AS mincr " + \
    "ON maxcr.GenreMax = mincr.GenreMin AND maxcr.UserIDMax = mincr.UserIDMin AND maxcr.RatingCountMax = mincr.RatingCountMin " + \
    "ORDER BY 1 ASC "
    

res = spark.sql(sqlString)
res.show()
res.coalesce(1).write.csv("hdfs://master:9000/output/q5.csv")

elapsed_time = (time.time() - start_time)
print("\n--- %s seconds ---\n" % elapsed_time)

f = open("output/sql/csv/times.txt", "a")
f.write("Q5:"+str(elapsed_time)+"\n")
f.close()