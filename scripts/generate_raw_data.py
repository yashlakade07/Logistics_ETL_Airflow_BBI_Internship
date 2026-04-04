import pandas as pd
import random
import os
import uuid
import argparse
import json
from datetime import datetime, timedelta

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATE_FILE = os.path.join(BASE_DIR, "../data/shipment_state.json")
OUTPUT_DIR = os.path.join(BASE_DIR, "../raw_data")

def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('execution_date', type=str)
    return parser.parse_args()

def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            return json.load(f)
    return []

def save_state(data):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, 'w') as f:
        json.dump(data, f, indent=4)

def check_previous_day_data(current_date):
    yesterday = current_date - timedelta(days=1)
    yesterday_str = yesterday.strftime("%Y-%m-%d")
    yesterday_file = os.path.join(OUTPUT_DIR, yesterday_str, f"shipments_{yesterday_str}.csv")
    return os.path.exists(yesterday_file)

def generate_data_for_date(target_date_str):
    print(f"--- Processing Data for Date: {target_date_str} ---")
    
    target_date = datetime.strptime(target_date_str, "%Y-%m-%d").date()
    
    all_shipments = load_state()
    daily_output_rows = []

    yesterday_data_exists = check_previous_day_data(target_date)

    if not yesterday_data_exists:
        print(f"[BOOTSTRAP] Starting fresh! Generating 100 initial records.")
        num_new_records = 100
    else:
        print(f"[INCREMENTAL] Routine run. Generating small batch.")
        num_new_records = random.randint(2, 5)

    for shipment in all_shipments:
        if shipment['status'] == 'In Transit':
            created_at = datetime.strptime(shipment['shipment_date'], "%Y-%m-%d").date()
            days_diff = (target_date - created_at).days
            
            should_deliver = False
            if days_diff >= 3:
                should_deliver = True
            else:
                should_deliver = random.choice([True, False])

            if should_deliver:
                shipment['status'] = 'Delivered'
                daily_output_rows.append(shipment.copy())

    categories = ['Auto Components', 'Food Containers', 'Medical Supplies', 'Electronics']
    origin_cities = ['Mumbai', 'Delhi', 'Bangalore', 'Chennai', 'Pune', 'Kolkata']
    destination_cities = ['Mumbai', 'Delhi', 'Bangalore', 'Chennai', 'Pune', 'Kolkata', None]

    for _ in range(num_new_records):
        origin = random.choice(origin_cities)
        dest = random.choice(destination_cities)
        if dest is None: dest = origin

        new_row = {
            'order_id': str(uuid.uuid4()),
            'product_category': random.choice(categories),
            'origin_city': origin,
            'destination_city': dest,  
            'shipment_date': target_date_str,
            'weight_kg': round(random.uniform(5.0, 500.0), 2),
            'shipping_cost': round(random.uniform(100.0, 5000.0), 2),
            'status': 'In Transit'
        }
        
        all_shipments.append(new_row)
        daily_output_rows.append(new_row)

    save_state(all_shipments)
    
    folder_path = os.path.join(OUTPUT_DIR, target_date_str)
    os.makedirs(folder_path, exist_ok=True)
    
    if daily_output_rows:
        df = pd.DataFrame(daily_output_rows)
        file_path = os.path.join(folder_path, f"shipments_{target_date_str}.csv")
        df.to_csv(file_path, index=False)
        print(f"Success! Saved {len(daily_output_rows)} rows to: {file_path}")
    else:
        print("No new data or updates for today.")

if __name__ == "__main__":
    args = parse_arguments()
    generate_data_for_date(args.execution_date)