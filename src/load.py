from logs.utils import logger

def load_data(df, table_name):
    if df is None:
        logger.error("No DataFrame provided to load")
        return

    jdbc_url = "jdbc:postgresql://localhost:5432/water_db"
    properties = {
        "user": "postgres",
        "password": "1234",
        "driver": "org.postgresql.Driver"
    }

    (
        df.write
        .mode("overwrite")
        .jdbc(url=jdbc_url, table=table_name, properties=properties)
    )

    logger.info(f"Loaded data into {table_name}")
