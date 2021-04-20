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
#from operators.upload_file import UploadFileOperator

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

# DAG
dag = DAG(
    'initial_dag',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
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
    repo_dir=repo_dir,
    file_name=file_name,
    year=year,
    output_dir=output_dir
) 


# year="2021"
# upload_file = UploadFileOperator(
#     task_id='upload_file',
#     dag=dag,
#     aws_credentials_id="aws_credentials",
#     S3_bucket="darties",
#     S3_key="sales_data",
#     year=year,
#     file_name=file_name, 
#     path_to_file=os.path.join(OUTPUTS_DIR, year, file_name) #complete path to file
# ) 


end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)




#start_operator >> clean_file  >> upload_file >>  end_operator

start_operator >> clean_file  >>  end_operator

