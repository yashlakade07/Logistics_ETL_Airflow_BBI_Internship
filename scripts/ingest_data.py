import pandas as pd
import sqlite3
import os
import argparse

def parse_arguments():
    parser = argparse.ArgumentParser(description="Ingest logistics data into SQLite.")
    
    parser.add_argument(
        'execution_date', 
        type=str, 
        help="The date of the data to ingest (YYYY-MM-DD)"
    )
    
    return parser.parse_args()

def run_ingestion(execution_date):
    
    BASE_DIR = "/home/yashl/airflow-local"
    DATA_DIR = os.path.join(BASE_DIR, "dags", "raw_data") 
    DB_NAME = os.path.join(BASE_DIR, "logistics.db")

    csv_filename = f"shipments_{execution_date}.csv"
    file_path = os.path.join(DATA_DIR, execution_date, csv_filename)

    print(f"--- STARTING INGESTION ---")
    print(f"Target Date: {execution_date}")
    print(f"Looking for file: {file_path}")

    if not os.path.exists(file_path):
        print(f"ERROR: File not found! {file_path}")
        print(f"Verify that generate_raw_data.py ran successfully for {execution_date}")
        exit(1)

    try:
        df = pd.read_csv(file_path)
        print(f"Successfully read {len(df)} rows.")
    except Exception as e:
        print(f"Error reading CSV: {e}")
        exit(1)

    if "destination_city" in df.columns and "origin_city" in df.columns:
        df["destination_city"] = df["destination_city"].combine_first(df["origin_city"])
        print("Applied COALESCE logic (filled null destinations).")

    try:
        conn = sqlite3.connect(DB_NAME)
        df.to_sql("shipments", conn, if_exists="append", index=False)
        conn.close()
        print("SUCCESS: Data loaded into SQLite database.")
    except Exception as e:
        print(f"Database Error: {e}")
        exit(1)

if __name__ == "__main__":
    args = parse_arguments()
    run_ingestion(args.execution_date)