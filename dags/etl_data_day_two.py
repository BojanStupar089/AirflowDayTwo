from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def extract():
    print("Extracting sales data from CSV...")

def transform():
    print("Cleaning and transforming sales data...")

def load():
    print("Loading sales data into Data Warehouse...")

with DAG(
    dag_id="etl_data_day_two",
    start_date=datetime(2025, 11, 1),
    schedule="@daily",
    catchup=False,
    description="Simple ETL demo DAG for day two",
) as dag:

    extract_task = PythonOperator(
        task_id="extract_step",
        python_callable=extract,
    )

    transform_task = PythonOperator(
        task_id="transform_step",
        python_callable=transform,
    )

    load_task = PythonOperator(
        task_id="load_step",
        python_callable=load,
    )

    extract_task >> transform_task >> load_task

