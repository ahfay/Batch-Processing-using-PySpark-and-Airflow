from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from modules.etl import *

with DAG(
    dag_id='top_countries',
    start_date=datetime(2024, 12, 18),
    schedule="@daily",
    catchup=False
) as dag:
    
    start_task = EmptyOperator(
        task_id='start'
    )
    
    op_extract_transform_top_countries = PythonOperator(
        task_id='extract_transform_top_countries',
        python_callable=extract_transform_top_countries
    )
    
    op_load_top_countries = PythonOperator(
        task_id='load_top_countries',
        python_callable=load_top_countries
    )
    
    end_task = EmptyOperator(  
        task_id='end'
    )
    
    # Atur dependensi task
    start_task >> op_extract_transform_top_countries >> op_load_top_countries >> end_task
