from datetime import datetime, timedelta
import os
from os import listdir
from os.path import isfile, join

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from operators.clean_file import CleanFileOperator
from operators.upload_file import UploadFileOperator
from operators.load_currency import LoadCurrencyOperator
from operators.load_cities import LoadCitiesOperator
from operators.load_mapping_regions import LoadMappingOperator

from infrastructure.SalesData import SalesData
from infrastructure.UserData import UserData
from infrastructure.StoreData import StoreData

from helpers import SqlQueries

import config.config as cf


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


dag = DAG(
    'initial_dag',
    default_args=default_args,
    description='Clean data from original file and produces cleaned files before upload to S3.',
    schedule_interval='0 * * * *',
    catchup=True,
    max_active_runs=1
)


start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


load_mapping_regions = LoadMappingOperator(
    task_id='load_mapping_regions',
    dag=dag
)


load_cities = LoadCitiesOperator(
    task_id='load_cities',
    dag=dag
)


load_currency = LoadCurrencyOperator(
    task_id='load_currency',
    dag=dag
)


milestone_1 = DummyOperator(task_id='milestone_1',  dag=dag)


sales_clean_file = CleanFileOperator(
    task_id='sales_clean_file',
    dag=dag,
    item='sales',
    UtilityClass=SalesData,
)


user_clean_file = CleanFileOperator(
    task_id='user_clean_file',
    dag=dag,
    item='users',
    UtilityClass=UserData,
)


store_clean_file = CleanFileOperator(
    task_id='store_clean_file',
    dag=dag,
    item='store',
    UtilityClass=StoreData,
)


def check_no_null(ds, **kwargs):
    """Print the Airflow context and ds variable from the context."""
    pprint(kwargs)
    print(ds)
    return 'Whatever you return gets printed in the logs'

check_no_null_values = PythonOperator(
    task_id='check_no_null_values',
    dag=dag,
    python_callable=check_no_null,
)


upload_file = UploadFileOperator(
    task_id='upload_file',
    dag=dag,
    aws_credentials_id="aws_credentials",
    S3_bucket="darties"
)


end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)





start_operator >> \
[ 
    load_mapping_regions, load_cities, load_currency
] >> \
milestone_1 >> \
[
    sales_clean_file, user_clean_file, store_clean_file
] >> \
check_no_null_values >> \
upload_file >> \
end_operator

