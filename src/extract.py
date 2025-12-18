from pyspark.sql import SparkSession
from logs.utils import logger

def extract_csv():
    spark = (
        SparkSession.builder
        .appName("WaterETL")
        .config(
            "spark.jars.packages",
            "org.postgresql:postgresql:42.7.3"
        )
        .getOrCreate()
    )

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv("data/water_potability.csv")
    )

    logger.info(f"Extracted {df.count()} rows")
    return df
