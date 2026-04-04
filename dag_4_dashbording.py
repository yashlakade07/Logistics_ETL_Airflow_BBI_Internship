from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator 
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta


PREVIOUS_DAG_ID = 'dag_3_transformation_sensor' 

default_args = {
    'owner': 'intern_user',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dag_4_dashbord_sensor',
    default_args=default_args,
    catchup=False,
    tags=['logistics', 'phase4']
) as dag:
    
    run_dashbord_script = BashOperator(
        task_id='run_dashbord_script',
        bash_command='python3 /home/yashl/airflow-local/dags/scripts/dashbord.py {{ ds }}'
    )


    run_dashbord_script

