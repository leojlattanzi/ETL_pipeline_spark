import pandas as pd
import psycopg2
from logs.utils import logger

def load_data(cleaned_rows, rejected_rows=None):
    """
    Load cleaned and rejected water potability data into PostgreSQL.
    cleaned_rows: pandas DataFrame with cleaned data (must not be None)
    rejected_rows: pandas DataFrame with rejected rows (optional)
    """

    if cleaned_rows is None or cleaned_rows.empty:
        logger.error("No cleaned data provided to load into PostgreSQL")
        return

    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host="localhost",
        database="water_db",
        user="postgres",
        password="1234"
    )
    cur = conn.cursor()

    # water_potability_cleaned tabke
    cur.execute("""
    CREATE TABLE IF NOT EXISTS water_potability_cleaned (
        id INT PRIMARY KEY,
        ph FLOAT,
        hardness FLOAT,
        solids FLOAT,
        chloramines FLOAT,
        sulfate FLOAT,
        conductivity FLOAT,
        organic_carbon FLOAT,
        trihalomethanes FLOAT,
        turbidity FLOAT,
        potability INT
    );
    """)

    # Insert cleaned data into rows
    for index, row in cleaned_rows.iterrows():
        cur.execute("""
            INSERT INTO water_potability_cleaned (
                id, ph, hardness, solids, chloramines, sulfate, conductivity, 
                organic_carbon, trihalomethanes, turbidity, potability
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING;
        """, (
            int(index),
            float(row["ph"]) if "ph" in row and pd.notna(row["ph"]) else None,
            float(row["hardness"]) if "hardness" in row and pd.notna(row["hardness"]) else None,
            float(row["solids"]) if "solids" in row and pd.notna(row["solids"]) else None,
            float(row["chloramines"]) if "chloramines" in row and pd.notna(row["chloramines"]) else None,
            float(row["sulfate"]) if "sulfate" in row and pd.notna(row["sulfate"]) else None,
            float(row["conductivity"]) if "conductivity" in row and pd.notna(row["conductivity"]) else None,
            float(row["organic_carbon"]) if "organic_carbon" in row and pd.notna(row["organic_carbon"]) else None,
            float(row["trihalomethanes"]) if "trihalomethanes" in row and pd.notna(row["trihalomethanes"]) else None,
            float(row["turbidity"]) if "turbidity" in row and pd.notna(row["turbidity"]) else None,
            int(row["potability"]) if "potability" in row and pd.notna(row["potability"]) else None
        ))

    logger.info(f"Loaded {len(cleaned_rows)} cleaned rows into PostgreSQL")

    # water_potability_rejected table
    if rejected_rows is not None and not rejected_rows.empty:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS water_potability_rejected (
            id SERIAL PRIMARY KEY,
            ph FLOAT,
            hardness FLOAT,
            solids FLOAT,
            chloramines FLOAT,
            sulfate FLOAT,
            conductivity FLOAT,
            organic_carbon FLOAT,
            trihalomethanes FLOAT,
            turbidity FLOAT,
            potability INT
        );
        """)

        cols = ['ph','hardness','solids','chloramines','sulfate',
                'conductivity','organic_carbon','trihalomethanes',
                'turbidity','potability']

        for _, row in rejected_rows.iterrows():
            cur.execute("""
                INSERT INTO water_potability_rejected (
                    ph, hardness, solids, chloramines, sulfate, conductivity,
                    organic_carbon, trihalomethanes, turbidity, potability
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, [float(row[c]) if c in row and pd.notna(row[c]) else None for c in cols])

        logger.info(f"Loaded {len(rejected_rows)} rejected rows into PostgreSQL")

    # commit + close connection
    conn.commit()
    cur.close()
    conn.close()
    logger.info("Data successfully loaded into PostgreSQL.")