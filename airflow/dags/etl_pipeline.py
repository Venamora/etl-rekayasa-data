from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import pandas as pd

# Database connection details
DB_USER = 'postgres'
DB_PASSWORD = 'Yefta21n0404'
DB_HOST = 'postgres'
DB_PORT = '5432'
DB_NAME = 'rekdat'
TABLE_NAME = 'table_rekdat'

# Function definitions
def extract_data(**kwargs):
    # Simulate extraction from Twitter and Weatherbit API
    tweets = pd.DataFrame({'datetime': ['2024-09-20'], 'text': ['Sunny weather in Jogja']})
    weather = pd.DataFrame({'datetime': ['2024-09-20'], 'temperature': [30], 'humidity': [70]})
    kwargs['ti'].xcom_push(key='tweets', value=tweets)
    kwargs['ti'].xcom_push(key='weather', value=weather)

def transform_data(**kwargs):
    ti = kwargs['ti']
    tweets = ti.xcom_pull(key='tweets', task_ids='extract_data')
    weather = ti.xcom_pull(key='weather', task_ids='extract_data')
    
    # Transform the data
    weather['datetime'] = pd.to_datetime(weather['datetime'])
    tweets['datetime'] = pd.to_datetime(tweets['datetime'])
    merged_data = pd.merge(weather, tweets, on='datetime', how='left')
    ti.xcom_push(key='final_data', value=merged_data)

def load_data(**kwargs):
    ti = kwargs['ti']
    final_data = ti.xcom_pull(key='final_data', task_ids='transform_data')
    
    # Database connection
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
    
    # Load data into PostgreSQL
    final_data.to_sql(TABLE_NAME, engine, if_exists='replace', index=False)
    print(f"Data loaded into {TABLE_NAME} successfully.")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'etl_pipeline',
    default_args=default_args,
    description='ETL Pipeline DAG',
    schedule_interval=None,  # Run manually or define your schedule
    start_date=datetime(2024, 11, 1),
    catchup=False,
) as dag:

    extract_data_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
    )

    transform_data_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )

    load_data_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
    )

    extract_data_task >> transform_data_task >> load_data_task
