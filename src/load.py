import pandas as pd
import psycopg2
from logs.utils import logger


def load_data(cleaned_rows, rejected_rows=None):

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

    
    # cleaned_rows table
    
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
        potability INT,
        updated_at TIMESTAMP DEFAULT NOW()
    );
    """)

    UPSERT_CLEANED = """
        INSERT INTO water_potability_cleaned (
            id, ph, hardness, solids, chloramines, sulfate, conductivity, 
            organic_carbon, trihalomethanes, turbidity, potability, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (id)
        DO UPDATE SET
            ph = EXCLUDED.ph,
            hardness = EXCLUDED.hardness,
            solids = EXCLUDED.solids,
            chloramines = EXCLUDED.chloramines,
            sulfate = EXCLUDED.sulfate,
            conductivity = EXCLUDED.conductivity,
            organic_carbon = EXCLUDED.organic_carbon,
            trihalomethanes = EXCLUDED.trihalomethanes,
            turbidity = EXCLUDED.turbidity,
            potability = EXCLUDED.potability,
            updated_at = NOW();
    """

    for index, row in cleaned_rows.iterrows():
        cur.execute(UPSERT_CLEANED, (
            int(index),
            float(row["ph"]) if pd.notna(row.get("ph")) else None,
            float(row["hardness"]) if pd.notna(row.get("hardness")) else None,
            float(row["solids"]) if pd.notna(row.get("solids")) else None,
            float(row["chloramines"]) if pd.notna(row.get("chloramines")) else None,
            float(row["sulfate"]) if pd.notna(row.get("sulfate")) else None,
            float(row["conductivity"]) if pd.notna(row.get("conductivity")) else None,
            float(row["organic_carbon"]) if pd.notna(row.get("organic_carbon")) else None,
            float(row["trihalomethanes"]) if pd.notna(row.get("trihalomethanes")) else None,
            float(row["turbidity"]) if pd.notna(row.get("turbidity")) else None,
            int(row["potability"]) if pd.notna(row.get("potability")) else None
        ))

    logger.info(f"Loaded {len(cleaned_rows)} cleaned rows into PostgreSQL")

    
    # rejected_rows table
   
    if rejected_rows is not None and not rejected_rows.empty:

        cur.execute("""
        CREATE TABLE IF NOT EXISTS water_potability_rejected (
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
            potability INT,
            updated_at TIMESTAMP DEFAULT NOW()
        );
        """)

        UPSERT_REJECTED = """
            INSERT INTO water_potability_rejected (
                id, ph, hardness, solids, chloramines, sulfate, conductivity,
                organic_carbon, trihalomethanes, turbidity, potability, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (id)
            DO UPDATE SET
                ph = EXCLUDED.ph,
                hardness = EXCLUDED.hardness,
                solids = EXCLUDED.solids,
                chloramines = EXCLUDED.chloramines,
                sulfate = EXCLUDED.sulfate,
                conductivity = EXCLUDED.conductivity,
                organic_carbon = EXCLUDED.organic_carbon,
                trihalomethanes = EXCLUDED.trihalomethanes,
                turbidity = EXCLUDED.turbidity,
                potability = EXCLUDED.potability,
                updated_at = NOW();
        """

        for index, row in rejected_rows.iterrows():
            cur.execute(UPSERT_REJECTED, (
                int(index),
                float(row["ph"]) if pd.notna(row["ph"]) else None,
                float(row["Hardness"]) if pd.notna(row["Hardness"]) else None,
                float(row["Solids"]) if pd.notna(row["Solids"]) else None,
                float(row["Chloramines"]) if pd.notna(row["Chloramines"]) else None,
                float(row["Sulfate"]) if pd.notna(row["Sulfate"]) else None,
                float(row["Conductivity"]) if pd.notna(row["Conductivity"]) else None,
                float(row["Organic_carbon"]) if pd.notna(row["Organic_carbon"]) else None,
                float(row["Trihalomethanes"]) if pd.notna(row["Trihalomethanes"]) else None,
                float(row["Turbidity"]) if pd.notna(row["Turbidity"]) else None,
                int(row["Potability"]) if pd.notna(row["Potability"]) else None
            ))

        logger.info(f"Loaded {len(rejected_rows)} rejected rows into PostgreSQL")

    else:
        logger.info("No rejected rows to load into PostgreSQL")

    # Commit + Close
    conn.commit()
    cur.close()
    conn.close()

    logger.info("Data successfully loaded into PostgreSQL.")
