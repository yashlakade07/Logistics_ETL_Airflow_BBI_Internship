import pandas as pd
import sqlite3
import os
import argparse

def parse_arguments():
    parser = argparse.ArgumentParser(description="Transform logistics data into category-specific tables.")
    parser.add_argument('execution_date', type=str, help="Date to process (YYYY-MM-DD)")
    return parser.parse_args()

def run_transformation(execution_date):
    print(f"--- STARTING CATEGORY SPLIT for {execution_date} ---")

    BASE_DIR = "/home/yashl/airflow-local"
    DB_NAME = os.path.join(BASE_DIR, "logistics.db")
    
    try:
        conn = sqlite3.connect(DB_NAME)
        print("Connected to database.")
    except Exception as e:
        print(f"DB Connection Error: {e}")
        exit(1)

    query = f"SELECT * FROM shipments WHERE shipment_date = '{execution_date}'"
    try:
        df = pd.read_sql(query, conn)
        print(f"Extracted {len(df)} rows for {execution_date}.")
        
        if df.empty:
            print("No data found for this date. Exiting.")
            conn.close()
            exit(0)
    except Exception as e:
        print(f"Read Error: {e}")
        conn.close()
        exit(1)

    categories = ['Auto Components', 'Food Containers', 'Medical Supplies', 'Electronics']

    try:
        for category in categories:
            print(f"Processing Category: {category}...")
            
            df_category = df[df['product_category'] == category].copy()
            
            if df_category.empty:
                print(f"  -> No data for {category}, skipping.")
                continue

            safe_name = category.lower().replace(" ", "_")
            table_name = f"target_{safe_name}"
            
            df_category.to_sql(table_name, conn, if_exists='append', index=False)
            print(f"  -> Loaded {len(df_category)} rows into table '{table_name}'")

        print("SUCCESS: Category-based tables created.")

    except Exception as e:
        print(f"Transformation Error: {e}")
        exit(1)
    finally:
        conn.close()

if __name__ == "__main__":
    args = parse_arguments()
    run_transformation(args.execution_date)