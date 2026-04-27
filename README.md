# End-to-End Logistics Data Pipeline

## 📊 Dashboard Preview
<img width="1195" height="709" alt="Screenshot 2026-04-27 182913" src="https://github.com/user-attachments/assets/378e921f-b426-478d-b3a5-adde81c9d640" />

<img width="1190" height="698" alt="Screenshot 2026-04-27 182926" src="https://github.com/user-attachments/assets/9b37bdf8-875f-4323-8a8d-cd5b1b8b9771" />

 

## 📌 Project Overview
This project was developed during my **Data Engineering Internship at BBI**[cite: 26]. [cite_start]I designed and deployed an automated data ingestion pipeline using **Apache Airflow** and **Python**, transitioning operations from manual CSVs to a centralized **SQLite** database.

## 🛠️ Tech Stack
* **Orchestration:** Apache Airflow 
* **Language:** Python 
* **Database:** SQLite 
* **Data Libraries:** Pandas, Matplotlib 
* **State Management:** JSON-based local storage 

## 🚀 Key Features
* **Automated ETL:** Developed dynamic scripts to automate real-world ETL orchestration.
* **Stateful Tracking:** Built a system to simulate real-world logistics data generation and maintain active shipment states.
* **SCD Type 2 Logic:** Implemented Slowly Changing Dimension logic to automatically transition shipment statuses while preserving historical records.
* **BI Reporting:** Created a workflow to process rolling 10-day shipment data and generate multi-page PDF dashboards.

## 📈 Impact
* Enabled **100% data traceability** for stakeholders through a centralized system.
* Replaced manual, error-prone CSV operations with a fully automated pipeline.
