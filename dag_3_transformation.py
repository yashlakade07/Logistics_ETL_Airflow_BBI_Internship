from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator 
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

PREVIOUS_DAG_ID = 'dag_2_ingestion_sensor'

default_args = {
    'owner': 'intern_user',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dag_3_transformation_sensor',
    default_args=default_args, 
    catchup=False,
    tags=['logistics', 'phase3']
) as dag:


    run_transformation_logic = BashOperator(
        task_id='run_transformation_script',
        bash_command='python3 /home/yashl/airflow-local/dags/scripts/transform_data.py {{ ds }}'
    )

    trigger_dag_4 = TriggerDagRunOperator(
        task_id='trigger_dashbording_dag',
        trigger_dag_id='dag_4_dashbord_sensor', 
        wait_for_completion=False,  
        reset_dag_run=True,         
        execution_date='{{ ds }}',
        conf = {'name': '{{ds}}'}
    )

    run_transformation_logic >> trigger_dag_4