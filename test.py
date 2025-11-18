import os
from logs.utils import logger
import pytest
import pandas as pd
from src.extract import extract_csv
from src.transform import transform_data
from src.load import load_data

# -----------------------------
# Test Extraction
# -----------------------------
def test_extract_csv_file_exists():
    file_path = "data/water_potability.csv"
    df = extract_csv(file_path)
    assert df is not None, "DataFrame should not be None"
    assert len(df) > 0, "CSV should contain rows"
    expected_cols = ['ph', 'Hardness', 'Solids', 'Chloramines', 'Sulfate',
                     'Conductivity', 'Organic_carbon', 'Trihalomethanes', 'Turbidity', 'Potability']
    assert all(col in df.columns for col in expected_cols), "Expected columns missing"

def test_extract_csv_file_missing():
    file_path = "data/missing_file.csv"
    df = extract_csv(file_path)
    assert df is None, "DataFrame should be None if file does not exist"

# -----------------------------
# Test Transformation
# -----------------------------
def test_transform_basic():
    # Sample data with some invalid/missing values
    data = {
        'ph': [7.0, None, 8.0],
        'hardness': [150, 200, -5],
        'solids': [2000, 3000, 2500],
        'chloramines': [7, 8, 9],
        'sulfate': [300, 400, 350],
        'conductivity': [400, 500, 450],
        'organic_carbon': [15, 20, 18],
        'trihalomethanes': [80, 90, 85],
        'turbidity': [3, 4, 3.5],
        'potability': [1, 0, 1]
    }
    df = pd.DataFrame(data)
    cleaned, rejected = transform_data(df)

    # Cleaned checks
    assert cleaned is not None
    assert len(cleaned) > 0
    assert all(cleaned['ph'] > 0), "All pH values should be positive"

    # Rejected checks
    assert rejected is not None
    assert len(rejected) > 0, "There should be rejected rows due to NaN or negative values"

def test_transform_empty_dataframe():
    df = pd.DataFrame(columns=['ph','hardness','solids','chloramines','sulfate','conductivity',
                               'organic_carbon','trihalomethanes','turbidity','potability'])
    cleaned, rejected = transform_data(df)
    assert cleaned.empty, "Cleaned DataFrame should be empty"
    assert rejected.empty, "Rejected DataFrame should be empty"

# -----------------------------
# Test Load
# -----------------------------
def test_load_cleaned_rows():
    # Minimal sample data for load
    data = {
        'ph': [7.0],
        'hardness': [150],
        'solids': [2000],
        'chloramines': [7],
        'sulfate': [300],
        'conductivity': [400],
        'organic_carbon': [15],
        'trihalomethanes': [80],
        'turbidity': [3],
        'potability': [1]
    }
    df = pd.DataFrame(data)

    try:
        load_data(df)  # Ensure this runs without exceptions
    except Exception as e:
        pytest.fail(f"load_data raised an exception: {e}")

# -----------------------------
# Integration Test
# -----------------------------
def test_full_etl_pipeline():
    # Run extraction
    extracted = extract_csv("data/water_potability.csv")
    assert extracted is not None

    # Run transformation
    cleaned, rejected = transform_data(extracted)
    assert cleaned is not None
    assert rejected is not None

    # Load cleaned rows
    try:
        load_data(cleaned, rejected_rows=rejected)
    except Exception as e:
        pytest.fail(f"Full ETL load raised an exception: {e}")
