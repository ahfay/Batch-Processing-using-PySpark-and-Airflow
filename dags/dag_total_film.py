from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from modules.etl import *

with DAG(
    dag_id='total_film',
    start_date=datetime(2022, 5, 28),
    schedule_interval='00 23 * * *',
    catchup=False
) as dag:
    
    start_task = EmptyOperator(
        task_id='start'
    )
    
    op_extract_transform_total_film = PythonOperator(
        task_id='extract_transform_total_film',
        python_callable=extract_transform_total_film
    )
    
    op_load_total_film = PythonOperator(
        task_id='load_total_film',
        python_callable=load_total_film
    )
    
    end_task = EmptyOperator(  
        task_id='end'
    )
    
    # Atur dependensi task
    start_task >> op_extract_transform_total_film >> op_load_total_film >> end_task
