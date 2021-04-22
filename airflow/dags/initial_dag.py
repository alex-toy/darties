from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator
from operators.clean_file import CleanFileOperator
from operators.upload_file import UploadFileOperator

from helpers import SqlQueries

output_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../output'))
file_name = "sales.json"


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
    'initial_dag',
    default_args=default_args,
    description='Clean data from original file and produces cleaned files before upload to S3.',
    schedule_interval='0 * * * *',
    catchup=True,
    max_active_runs=1
)


start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


year="2021"
clean_file = CleanFileOperator(
    task_id='clean_file',
    dag=dag,
    year=year
)


year = "2021"
file_name = 'cleaned_2021_sales.json'
output_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../output'))
upload_file = UploadFileOperator(
    task_id='upload_file',
    dag=dag,
    aws_credentials_id="aws_credentials",
    S3_bucket="darties"
)


end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> clean_file >> upload_file >> end_operator

