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

from operators.stage_redshift import StageToRedshiftOperator
from operators.unstage_from_redshift import UnstageFromRedshiftOperator
from operators.load_mapping_regions import LoadMappingOperator
from operators.build_dimension import BuildDimensionOperator
from operators.update_dimension import UpdateDimensionOperator
from operators.clean_file import CleanFileOperator
from operators.check_null import CheckNullOperator
from operators.check_positive import CheckPositiveOperator

from infrastructure.SalesData import SalesData
from infrastructure.UserData import UserData
from infrastructure.StoreData import StoreData

from operators.load_fact import LoadFactOperator


import config.config as cf


from helpers import SqlQueries
from helpers import UpdateSqlQueries


default_args = {
    'owner': 'alex-toy',
    'start_date': datetime(2020, 1, 1),
    'end_date': datetime(2021, 12, 31),
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


unstage_to_S3s = []
dimension_items = [
    "staging_utilisateur"
]


for dimension_item in dimension_items :
    unstage_to_S3 = UnstageFromRedshiftOperator(
        task_id=f"unstage_{dimension_item}_to_S3",
        dag=dag,
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        S3_bucket="darties",
        table=dimension_item
    )
    unstage_to_S3s.append(unstage_to_S3)


end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> \
[
    utS for utS in unstage_to_S3s
] >> end_operator



