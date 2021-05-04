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


def check_action(folder, year, file) :
    path = join(cf.OUTPUTS_DIR, folder, year, file)
    print(f"path : {path} - file : {file}")
    with open(path) as json_file:
        data = json.load(json_file)
        print(data)



def check_no_null():
    
    folders = [f for f in listdir(cf.OUTPUTS_DIR) if isdir(join(cf.OUTPUTS_DIR, f))]
    for folder in folders :
        inner_folder = Path(join(cf.OUTPUTS_DIR, folder))
        year_folders = [f for f in listdir(inner_folder) if isdir(join(cf.OUTPUTS_DIR, folder, f))]
        for year_folder in year_folders :
            file_path = join(cf.OUTPUTS_DIR, folder, year_folder)
            files = [f for f in listdir(file_path) if isfile(join(file_path, f)) and not f.startswith('.')]
            for file in files :
                check_action(folder, year_folder, file)
                



check_no_null_values = PythonOperator(
    task_id='check_no_null_values',
    dag=dag,
    python_callable=check_no_null,
)


end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> check_no_null_values >> end_operator



