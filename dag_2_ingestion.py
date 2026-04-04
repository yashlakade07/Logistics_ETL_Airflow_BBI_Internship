from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator 
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta


DAG_1_ID = 'dag_1_separate_file_trigger' 

default_args = {
    'owner': 'intern_user',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dag_2_ingestion_sensor',
    default_args=default_args, 
    catchup=False,
    tags=['logistics', 'phase2']
) as dag:
    
    run_ingestion_script = BashOperator(
        task_id='run_ingestion_script',
        bash_command='python3 /home/yashl/airflow-local/dags/scripts/ingest_data.py {{ ds }}'
    )

    trigger_dag_3 = TriggerDagRunOperator(
        task_id='trigger_transformation_dag',
        trigger_dag_id='dag_3_transformation_sensor', 
        wait_for_completion=False,  
        reset_dag_run=True,         
        execution_date='{{ ds }}',
        conf = {'name': '{{ds}}'}
    )

    run_ingestion_script >> trigger_dag_3

