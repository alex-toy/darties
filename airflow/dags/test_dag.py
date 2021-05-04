from datetime import datetime, timedelta
import os
from os import listdir
from os.path import isfile, join, isdir

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


def check_no_null():
    
    folders = [f for f in listdir(cf.OUTPUTS_DIR) if isdir(join(cf.OUTPUTS_DIR, f))]
    for inner_folder in folders :
        print(inner_folder)
        year_folders = [f for f in inner_folder if isdir(join(inner_folder, f))]
        print(f"year_folders : {year_folders}")
        for year_folder in year_folders :
            print(year_folder)
            files = [f for f in listdir(year_folder) if isfile(join(year_folder, f))]
            for file in files :
                print(file)




check_no_null_values = PythonOperator(
    task_id='check_no_null_values',
    dag=dag,
    python_callable=check_no_null,
)


end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> check_no_null_values >> end_operator



