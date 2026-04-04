from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator 
from datetime import datetime, timedelta

default_args = {
    'owner': 'intern_user',
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dag_1_separate_file_trigger',
    default_args=default_args,
    start_date=datetime(2026, 1, 27),
    schedule_interval='0 10 * * *',
    #schedule_interval='@daily',
    catchup=False,
    tags=['logistics', 'phase1']
) as dag:

    run_generator_script = BashOperator(
        task_id='trigger_generator_script',
        bash_command='python3 /home/yashl/airflow-local/dags/scripts/generate_raw_data.py {{ ds }}'
    )

    trigger_dag_2 = TriggerDagRunOperator(
        task_id='trigger_ingestion_dag',
        trigger_dag_id='dag_2_ingestion_sensor', 
        wait_for_completion=False,  
        reset_dag_run=True,         
        execution_date='{{ ds }}',
        conf = {'name': '{{ds}}'}
    )

    run_generator_script >> trigger_dag_2