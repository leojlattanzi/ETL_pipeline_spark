from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("PysparkApp")
    .getOrCreate()
)

data = [1,2,3,4,5]
rdd= spark.sparkContext.parallelize(data)
print(rdd.collect())
print(rdd.reduce(lambda x,y:x + y))

spark.stop()
