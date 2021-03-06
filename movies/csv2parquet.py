import sys
from pyspark.sql import SparkSession

csvPath = "hdfs://master:9000/movies/"+str(sys.argv[1])+".csv"
parquetFilename = "hdfs://master:9000/movies/"+str(sys.argv[1])+".parquet"

spark = SparkSession.builder.appName("Parquet").getOrCreate()

parquetDF = spark.read.csv(csvPath)
parquetDF.write.parquet(parquetFilename)

# df = pd.read_csv(csvPath)
# df.to_parquet(parquetFilename)