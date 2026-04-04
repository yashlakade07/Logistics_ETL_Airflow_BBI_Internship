import pandas as pd
import os
import argparse
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from datetime import datetime, timedelta

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DATA_DIR = os.path.join(BASE_DIR, "../raw_data")
OUTPUT_DIR = os.path.join(BASE_DIR, "../dashboard_plots")

def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("execution_date", type=str)
    return parser.parse_args()

def generate_reports(execution_date):
    print(f"--- Generating PDF Report for {execution_date} ---")
    
    end_date = datetime.strptime(execution_date, "%Y-%m-%d")
    df = pd.DataFrame()   

    print(f"Scanning raw_data folder: {RAW_DATA_DIR}")
    
    for i in range(10):
        d = end_date - timedelta(days=i)
        ds = d.strftime("%Y-%m-%d")
        
        file_path = os.path.join(RAW_DATA_DIR, ds, f"shipments_{ds}.csv")

        if not os.path.exists(file_path):
            print(f"  [Missing] {ds}")
            continue 
        
        print(f"  [Reading] {file_path}")
        daily_df = pd.read_csv(file_path)
        df = pd.concat([df, daily_df], ignore_index=True)

    if df.empty:
        print("No data found for the last 10 days.")
        return

    df['shipment_date'] = pd.to_datetime(df['shipment_date'])

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    pdf_path = os.path.join(OUTPUT_DIR, f"dashboard_report_{execution_date}.pdf")
    
    print(f"Creating PDF at: {pdf_path}")
    
    with PdfPages(pdf_path) as pdf:
        
        plt.figure(figsize=(10, 6))
        cat_trend = (
            df.groupby(['shipment_date', 'product_category'])
            .size()
            .unstack(fill_value=0)
            .sort_index()
        )
        
        colors = {"Electronics": "red", "Medical Supplies": "green", 
                  "Auto Components": "blue", "Food Containers": "orange"}
        
        cat_trend.plot(kind="line", marker="o", color=[colors.get(x, 'black') for x in cat_trend.columns], ax=plt.gca())
        
        plt.title("Daily Requests Trend by Category (Last 10 Days)")
        plt.xlabel("Date")
        plt.ylabel("Count")
        plt.xticks(rotation=45)
        plt.tight_layout()
        
        pdf.savefig()  
        plt.close()
        print("  - Added Page 1: Category Trend")

        plt.figure(figsize=(10, 6))
        trend = (
            df[df['status'] == "In Transit"]
            .groupby('shipment_date')
            .size()
            .sort_index()
        )
        
        if not trend.empty:
            trend.index = trend.index.strftime('%Y-%m-%d')
            trend.plot(kind="bar", color="orange", edgecolor="black")
            plt.title("In-Transit Shipments Trend (Last 10 Days)")
            plt.xlabel("Date")
            plt.ylabel("Count")
            plt.xticks(rotation=45)
            plt.tight_layout()
            
            pdf.savefig()  
            plt.close()
            print("  - Added Page 2: In-Transit Trend")
        else:
            print("  - Skipping Page 2: No 'In Transit' data found.")

    print(f"Success! PDF saved: {pdf_path}")

if __name__ == "__main__":
    args = parse_arguments()
    generate_reports(args.execution_date)