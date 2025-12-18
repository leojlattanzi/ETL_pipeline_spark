from src.extract import extract_csv
from src.transform import transform_data
from src.load import load_data
from logs.utils import logger

logger.info("ETL pipeline started")

df = extract_csv()

cleaned_df, rejected_df = transform_data(df)

load_data(cleaned_df, "water_potability_cleaned")
load_data(rejected_df, "water_potability_rejected")

logger.info("ETL pipeline completed successfully")
