import pandas as pd
from logs.utils import logger

def transform_data(df: pd.DataFrame):
    if df is None:
        logger.error("No DataFrame provided for transformation")
        return None, None

    logger.info(f"Starting transformation on {len(df)} rows")

    # rejected rows
    rejects = pd.DataFrame(columns=df.columns)
   
    # remove missing values
    initial_rows = len(df)
    missing_rows = df[df.isnull().any(axis=1)]
    rejects = pd.concat([rejects, missing_rows])
    df = df.dropna()
    logger.info(f"Dropped {len(missing_rows)} rows with missing values")

    # Remove duplicate rows
    initial_rows = len(df)
    duplicate_rows = df[df.duplicated()]
    rejects = pd.concat([rejects, duplicate_rows])
    df = df.drop_duplicates()
    logger.info(f"Dropped {len(duplicate_rows)} duplicate rows")

    # Basic numeric validation (no negative values)
    numeric_columns = df.select_dtypes(include=['float64', 'int64']).columns
    for col in numeric_columns:
        invalid_rows = df[df[col] < 0]
        if not invalid_rows.empty:
            rejects = pd.concat([rejects, invalid_rows])
            df = df[df[col] >= 0]
            logger.info(f"Dropped {len(invalid_rows)} rows with invalid {col} < 0")

    # Standardize column names
    df.columns = [str(col).strip().lower() for col in df.columns]

    # Standardize columns
    string_cols = df.select_dtypes(include='object').columns
    for col in string_cols:
        df[col] = df[col].str.strip().str.lower()

    logger.info(f"Transformation complete. {len(df)} rows remaining")
    logger.info(f"Total rejected rows: {len(rejects)}")

    return df, rejects