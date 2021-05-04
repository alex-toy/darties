from datetime import datetime, timedelta
import os
from os import listdir
from os.path import isfile, join, isdir, abspath
import json
from pathlib import Path

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

from operators.load_mapping_regions import LoadMappingOperator
from operators.build_dimension import BuildDimensionOperator
from operators.clean_file import CleanFileOperator

from infrastructure.SalesData import SalesData
from infrastructure.UserData import UserData
from infrastructure.StoreData import StoreData


import config.config as cf


from helpers import SqlQueries


default_args = {
    'owner': 'alex-toy',
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
    description='test dags for debugging purpose.',
    schedule_interval='0 * * * *',
    catchup=True,
    max_active_runs=1
)


start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


load_time_dimension_table = BuildDimensionOperator(
    task_id='load_time_dimension_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="temps",
    append=False
)


end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> load_time_dimension_table >> end_operator



