# Water Potability ETL Pipeline

This project is an **ETL (Extract, Transform, Load) pipeline** for cleaning, validating, and loading water potability data into a PostgreSQL database. It also saves both cleaned and rejected rows as CSV files for auditing purposes.

---

## Table of Contents

* [Project Structure](#project-structure)
* [Features](#features)
* [Prerequisites](#prerequisites)
* [Setup](#setup)
* [Usage](#usage)
* [ETL Flow](#etl-flow)
* [Database Schema](#database-schema)
* [Logging](#logging)
* [Output](#output)
* [Future Improvements](#future-improvements)

---

## Project Structure

```
ETL_pipeline/
├── src/
│   ├── extract.py         # Extracts CSV data
│   ├── transform.py       # Cleans and validates data
│   ├── load.py            # Loads cleaned & rejected data into PostgreSQL
├── logs/
│   └── utils.py           # Logging utility
├── data/
│   └── water_potability.csv
├── output/
│   ├── cleaned_rows.csv
│   └── rejected_rows.csv
├── main.py                # Runs the ETL pipeline
├── README.md
```

---

## Features

* Extracts water potability data from CSV.
* Cleans and transforms data:

  * Drops missing or duplicate rows
  * Validates numeric values (no negatives)
  * Standardizes column names and string values
* Saves **cleaned** and **rejected rows** to CSV for auditing.
* Loads data into **PostgreSQL**:

  * `water_potability_cleaned` table
  * `water_potability_rejected` table

---

## Prerequisites

* Python 3.10+
* PostgreSQL 18+ (or compatible version)
* Python packages: 
   -Pandas
   -Psycopg2
   -Pytest     #------Testing Libraries
   -Magic Mock #---/

## Setup

1. **Clone the repository:**

```bash
git clone <your-repo-url>
cd ETL_pipeline
```

2. **Create a virtual environment and activate it:**

```bash
python -m venv venv
# Windows
venv\Scripts\activate
# macOS/Linux
source venv/bin/activate
```

3. **Install dependencies:**

```bash
pip install -r requirements.txt
```

4. **Ensure PostgreSQL is running** and a database is created:

```sql
CREATE DATABASE water_db;
```

5. **Update database credentials** in `load.py` if needed:

```python
conn = psycopg2.connect(
    host="localhost",
    database="water_db",
    user="postgres",
    password="your_password"
)
```

---

## Usage

Run the ETL pipeline:

```bash
python main.py
```

* Extracts data from `data/water_potability.csv`
* Transforms it (cleaning & validation)
* Saves cleaned & rejected rows to CSV
* Loads both cleaned and rejected data into PostgreSQL

---

## ETL Flow

1. **Extract**:
   Reads the CSV into a pandas DataFrame.

2. **Transform**:

   * Drops rows with missing or negative values
   * Removes duplicates
   * Standardizes column names and strings
   * Splits data into `cleaned` and `rejected`

3. **Load**:

   * Creates tables if not exist
   * Loads cleaned rows into `water_potability_cleaned`
   * Loads rejected rows into `water_potability_rejected`

---

## Database Schema

### `water_potability_cleaned`

| Column          | Type            |
| --------------- | --------------- |
| id              | INT PRIMARY KEY |
| ph              | FLOAT           |
| hardness        | FLOAT           |
| solids          | FLOAT           |
| chloramines     | FLOAT           |
| sulfate         | FLOAT           |
| conductivity    | FLOAT           |
| organic_carbon  | FLOAT           |
| trihalomethanes | FLOAT           |
| turbidity       | FLOAT           |
| potability      | INT             |

### `water_potability_rejected`

| Column          | Type               |
| --------------- | ------------------ |
| id              | SERIAL PRIMARY KEY |
| ph              | FLOAT              |
| hardness        | FLOAT              |
| solids          | FLOAT              |
| chloramines     | FLOAT              |
| sulfate         | FLOAT              |
| conductivity    | FLOAT              |
| organic_carbon  | FLOAT              |
| trihalomethanes | FLOAT              |
| turbidity       | FLOAT              |
| potability      | INT                |

---

## Logging

All ETL steps are logged using `logs/utils.py`. Logs include:

* Number of rows extracted
* Rows dropped due to missing/duplicate/invalid values
* Rows loaded into PostgreSQL

---

## Output

* `output/cleaned_rows.csv` → Cleaned, valid data
* `output/rejected_rows.csv` → Rows rejected during transformation

---

## Future Improvements

* Add command-line arguments for flexible file paths and database credentials
* Add more advanced data validation
