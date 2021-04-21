from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators.clean_file import CleanFileOperator
from operators.upload_file import UploadFileOperator


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

# DAG
dag = DAG(
    'test_dag',
    default_args=default_args,
    description='Clean data from original file and produces cleaned files before upload to S3.',
    schedule_interval='0 * * * *',
    catchup=True,
    max_active_runs=1
)


start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


repo_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../data'))
file_name = '2020_HISTO.xlsx'
year="2021"
clean_file = CleanFileOperator(
    task_id='clean_file',
    dag=dag,
    repo_dir="",
    file_name="",
    year=""
)


end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


#start_operator >> clean_file >> end_operator
start_operator >> end_operator



