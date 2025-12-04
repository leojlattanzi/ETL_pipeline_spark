from src.extract import extract_csv
from src.transform import transform_data
from src.load import load_data
from logs.utils import logger

logger.info("ETL pipeline started")

# Extract
extracted = extract_csv()
if extracted is None:
    logger.error("Extraction failed")
    exit(1)
logger.info(extracted.head())

# Transform
cleaned, rejects = transform_data(extracted)

if cleaned is not None:
    logger.info("Data transformed successfully")
    logger.info(cleaned.head())
else:
    logger.error("Data transformation failed")
    exit(1)

# save cleaned and rejected .to_csv
if cleaned is not None and not cleaned.empty:
    cleaned.index.name = "id"
    cleaned.to_csv("output/cleaned_rows.csv", index_label="Id")
    logger.info(f"Saved {len(cleaned)} cleaned rows to output/cleaned_rows.csv")

if rejects is not None and not rejects.empty:
    rejects.to_csv("output/rejected_rows.csv", index_label="Id")
    logger.info(f"Saved {len(rejects)} rejected rows to output/rejected_rows.csv")

#load data into postgres
load_data(cleaned, rejected_rows=rejects)