import sqlite3
import pandas as pd
import os
import logging

def load_to_sqlite(db_path: str, table_name: str, parquet_path: str):
    if not os.path.exists(parquet_path):
        print(f"Parquet file {parquet_path} does not exist.")
        return

    # Read Parquet
    df = pd.read_parquet(parquet_path, engine="pyarrow")
    print(f"Loaded {len(df)} rows from {parquet_path}")
    print(df.head())

    # Connect to SQLite
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create table if not exists
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            product_id TEXT PRIMARY KEY,
            model_name TEXT,
            memory_str TEXT,
            memory_gb INTEGER,
            release_year INTEGER,
            color_en TEXT,
            price INTEGER
        )
    """)
    conn.commit()

    # Insert rows into SQLite
    try:
        df.to_sql(table_name, conn, if_exists='append', index=False)
        print(f"Inserted {len(df)} rows into table '{table_name}' in database '{db_path}'")
        logging.info(f"Data loading completed, {len(df)} rows inserted into {table_name}.")
    except Exception as e:
        print(f"Error inserting rows: {e}")

    conn.close()


def main():
    db_path = "/workspaces/SIS2/data/output.db"
    table_name = "Ispace_products"
    parquet_path = "/workspaces/SIS2/data/cleaned_data.parquet"

    load_to_sqlite(db_path, table_name, parquet_path)


if __name__ == "__main__":
    main()
