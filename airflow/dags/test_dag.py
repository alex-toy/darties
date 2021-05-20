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
from operators.load_mapping_regions import LoadMappingOperator
from operators.build_dimension import BuildDimensionOperator
from operators.update_dimension import UpdateDimensionOperator
from operators.clean_file import CleanFileOperator
from operators.check_null import CheckNullOperator
from operators.check_positive import CheckPositiveOperator

from infrastructure.SalesData import SalesData
from infrastructure.UserData import UserData
from infrastructure.StoreData import StoreData


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


update_ca_fours_table = UpdateDimensionOperator(
    task_id='update_ca_fours_table',
    dag=dag,
    redshift_conn_id="redshift",
    update_query=UpdateSqlQueries.update_query,
    kpi="ca_reel",
    item="fours",
    id_famille_produit=3,
    staging_monthly_table="staging_monthly_ca_fours"
)




end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> update_ca_fours_table >> end_operator



