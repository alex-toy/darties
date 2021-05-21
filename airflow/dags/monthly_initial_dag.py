from datetime import datetime, timedelta
import os
from os import listdir
from os.path import isfile, join, isdir, abspath
from pathlib import Path
import json

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from operators.clean_monthly_file import CleanMonthlyFileOperator
from operators.upload_monthly_file import UploadMonthlyFileOperator
from operators.load_currency import LoadCurrencyOperator
from operators.load_cities import LoadCitiesOperator
from operators.load_mapping_regions import LoadMappingOperator

from infrastructure.SalesData import SalesData
from infrastructure.MonthlySalesData import MonthlySalesData
from infrastructure.UserData import UserData
from infrastructure.StoreData import StoreData

from helpers import SqlQueries

import config.config as cf


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


dag = DAG(
    'monthly_initial_dag',
    default_args=default_args,
    description='Clean data from original monthly file and produces cleaned files before upload to S3.',
    schedule_interval='0 * * * *',
    catchup=True,
    max_active_runs=1
)


start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


#milestone_1 = DummyOperator(task_id='milestone_1',  dag=dag)


sales_clean_file = CleanMonthlyFileOperator(
    task_id='sales_clean_file',
    dag=dag,
    item='monthly_sales',
    UtilityClass=MonthlySalesData,
)



upload_file = UploadMonthlyFileOperator(
    task_id='upload_file',
    dag=dag,
    aws_credentials_id="aws_credentials",
    S3_bucket="darties"
)


end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)





start_operator >> \
sales_clean_file >> upload_file >> \
end_operator
