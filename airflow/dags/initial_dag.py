from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from operators.clean_file import CleanFileOperator
from operators.upload_file import UploadFileOperator
from operators.load_currency import LoadCurrencyOperator
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


dag = DAG(
    'initial_dag',
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


load_currency = LoadCurrencyOperator(
    task_id='load_currency',
    dag=dag
)


clean_file = CleanFileOperator(
    task_id='clean_file',
    dag=dag
)


upload_file = UploadFileOperator(
    task_id='upload_file',
    dag=dag,
    aws_credentials_id="aws_credentials",
    S3_bucket="darties"
)


end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> load_cities >> load_currency >> clean_file >> upload_file >> end_operator



