import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from src.extract import extract_csv
from src.transform import transform_data
from src.load import load_data

# Sample DataFrame for testing
sample_df = pd.DataFrame({
    "ph": [7.0, 8.0],
    "hardness": [150, 150],
    "solids": [200, 200],
    "chloramines": [3, 3],
    "sulfate": [250, 250],
    "conductivity": [300, 300],
    "organic_carbon": [1.5, 1.5],
    "trihalomethanes": [0.5, 0.5],
    "turbidity": [1.0, 1.0],
    "potability": [1, 1]
})


# Test extract_csv

@patch("src.extract.logger")
def test_extract_csv_success(mock_logger, tmp_path):
    # Create temporary CSV
    file = tmp_path / "test.csv"
    sample_df.to_csv(file, index=False)

    df = extract_csv(file)
    assert isinstance(df, pd.DataFrame)
    mock_logger.info.assert_called()

@patch("src.extract.logger")
def test_extract_csv_file_not_found(mock_logger):
    df = extract_csv("non_existent_file.csv")
    assert df is None
    mock_logger.error.assert_called()


# Test transform_data

@patch("src.transform.logger")
def test_transform_data_all(mock_logger):
    cleaned, rejected = transform_data(sample_df)
    assert isinstance(cleaned, pd.DataFrame)
    assert isinstance(rejected, pd.DataFrame)
    mock_logger.info.assert_called()

@patch("src.transform.logger")
def test_transform_data_none_df(mock_logger):
    cleaned, rejected = transform_data(None)
    assert cleaned is None
    assert rejected is None
    mock_logger.error.assert_called()

 #Test load_data

@patch("src.load.psycopg2.connect")
@patch("src.load.logger")
def test_load_data_clean_only(mock_logger, mock_connect):
    mock_conn = MagicMock() #-- make mocks
    mock_cur = MagicMock()
    mock_connect.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cur

    # Only cleaned data
    load_data(sample_df, rejected_rows=None)

    mock_logger.info.assert_any_call(f"Loaded {len(sample_df)} cleaned rows into PostgreSQL")
    mock_logger.info.assert_any_call("No rejected rows to load into PostgreSQL")

@patch("src.load.psycopg2.connect")
@patch("src.load.logger")
def test_load_data_none_clean(mock_logger, mock_connect):
    # Only rejected rows
    load_data(None, rejected_rows=sample_df)
    mock_logger.error.assert_called()
