from pyspark.sql import functions as F
from logs.utils import logger

def transform_data(df):
    total_rows = df.count()
    logger.info(f"Starting transformation on {total_rows} rows")

    # remove missing values
    
    not_null = None
    for col in df.columns:
        c = F.col(col).isNotNull()
        not_null = c if not_null is None else not_null & c

    missing_df = (
        df.filter(~not_null)
        .withColumn("reject_reason", F.lit("MISSING_VALUE"))
    )

    cleaned_df = df.filter(not_null)

    logger.info(f"Removed {missing_df.count()} Missing Values")

    
    # De-Dupe

    dedup_df = cleaned_df.dropDuplicates()
    duplicate_df = (
        cleaned_df.subtract(dedup_df)
        .withColumn("reject_reason", F.lit("DUPLICATE"))
    )

    cleaned_df = dedup_df

    logger.info(f"Removed {duplicate_df.count()} Duped values")


    # validation (no negative #s)

    numeric_cols = [
        f.name for f in cleaned_df.schema.fields
        if f.dataType.simpleString() in ["int", "double"]
    ]

    valid_condition = None
    for col in numeric_cols:
        c = F.col(col) >= 0
        valid_condition = c if valid_condition is None else valid_condition & c

    negative_df = (
        cleaned_df.filter(~valid_condition)
        .withColumn("reject_reason", F.lit("NEGATIVE_VALUE"))
    )

    cleaned_df = cleaned_df.filter(valid_condition)

    logger.info(f"Validated {df.count()} Rows")

    #rejected data
    rejected_df = (
        missing_df
        .unionByName(duplicate_df, allowMissingColumns=True)
        .unionByName(negative_df, allowMissingColumns=True)
    )

    logger.info(f"Cleaned rows: {cleaned_df.count()}")
    logger.info(f"Rejected rows: {rejected_df.count()}")

    return cleaned_df, rejected_df
