from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

from operators.load_cities import LoadCitiesOperator


from helpers import SqlQueries


default_args = {
    'owner': 'udacity',
    'start_date': datetime(2018, 11, 1),
    'end_date': datetime(2019, 12, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False,
}

DAG
dag = DAG(
    'test_dag',
    default_args=default_args,
    description='Clean data from original file and produces cleaned files before upload to S3.',
    schedule_interval='0 * * * *',
    catchup=True,
    max_active_runs=1
)


start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


load_cities = LoadCitiesOperator(
    task_id='load_cities',
    dag=dag
)


end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> load_cities >> end_operator



