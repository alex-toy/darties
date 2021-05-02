from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.build_dimension import BuildDimensionOperator
from operators.data_quality import DataQualityOperator

from helpers import SqlQueries

#OUTPUTS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../output'))


default_args = {
    'owner': 'udacity',
    'start_date': datetime(2018, 11, 1),
    'end_date': datetime(2019, 12, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False,
    'check_null_sql' : """SELECT COUNT(*) FROM {} WHERE {} IS NULL;""",
    'check_count_sql' : """SELECT COUNT(*) FROM {};"""
}

# DAG
dag = DAG(
    'global_dag',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *',
    catchup=True,
    max_active_runs=1
)


start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)



create_tables = PostgresOperator(
    task_id="create_tables",
    dag=dag,
    sql='create_tables.sql',
    postgres_conn_id="redshift"
)

stage_CA_Fours_to_redshift = StageToRedshiftOperator(
    task_id='stage_CA_Fours_to_redshift',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_CA_Fours",
    S3_bucket="darties",
    S3_key="CA_Fours",
    delimiter=",",
    formatting="JSON 'auto'"
)

stage_MB_Fours_to_redshift = StageToRedshiftOperator(
    task_id='stage_MB_Fours_to_redshift',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_MB_Fours",
    S3_bucket="darties",
    S3_key="MB_Fours",
    delimiter=",",
    formatting="JSON 'auto'"
)

stage_V_Fours_to_redshift = StageToRedshiftOperator(
    task_id='stage_V_Fours_to_redshift',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_V_Fours",
    S3_bucket="darties",
    S3_key="V_Fours",
    delimiter=",",
    formatting="JSON 'auto'"
)

milestone_1 = DummyOperator(task_id='milestone_1',  dag=dag)

stage_CA_Hifi_to_redshift = StageToRedshiftOperator(
    task_id='stage_CA_Hifi_to_redshift',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_CA_Hifi",
    S3_bucket="darties",
    S3_key="CA_Hifi",
    delimiter=",",
    formatting="JSON 'auto'"
)

stage_MB_Hifi_to_redshift = StageToRedshiftOperator(
    task_id='stage_MB_Hifi_to_redshift',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_MB_Hifi",
    S3_bucket="darties",
    S3_key="MB_Hifi",
    delimiter=",",
    formatting="JSON 'auto'"
)

stage_V_Hifi_to_redshift = StageToRedshiftOperator(
    task_id='stage_V_Hifi_to_redshift',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_V_Hifi",
    S3_bucket="darties",
    S3_key="V_Hifi",
    delimiter=",",
    formatting="JSON 'auto'"
)

stage_currency_to_redshift = StageToRedshiftOperator(
    task_id='stage_currency_to_redshift',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_currency",
    S3_bucket="darties",
    S3_key="currency",
    delimiter=",",
    formatting="JSON 'auto'"
)

milestone_2 = DummyOperator(task_id='milestone_2',  dag=dag)

stage_CA_Magneto_to_redshift = StageToRedshiftOperator(
    task_id='stage_CA_Magneto_to_redshift',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_CA_Magneto",
    S3_bucket="darties",
    S3_key="CA_Magneto",
    delimiter=",",
    formatting="JSON 'auto'"
)

stage_MB_Magneto_to_redshift = StageToRedshiftOperator(
    task_id='stage_MB_Magneto_to_redshift',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_MB_Magneto",
    S3_bucket="darties",
    S3_key="MB_Magneto",
    delimiter=",",
    formatting="JSON 'auto'"
)

stage_V_Magneto_to_redshift = StageToRedshiftOperator(
    task_id='stage_V_Magneto_to_redshift',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_V_Magneto",
    S3_bucket="darties",
    S3_key="V_Magneto",
    delimiter=",",
    formatting="JSON 'auto'"
)

stage_cities_to_redshift = StageToRedshiftOperator(
    task_id='stage_cities_to_redshift',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_V_Magneto",
    S3_bucket="darties",
    S3_key="staging_cities",
    delimiter=",",
    formatting="JSON 'auto'"
)

stage_currency_to_redshift = StageToRedshiftOperator(
    task_id='stage_currency_to_redshift',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_V_Magneto",
    S3_bucket="darties",
    S3_key="staging_currency",
    delimiter=",",
    formatting="JSON 'auto'"
)

load_time_dimension_table = BuildDimensionOperator(
    task_id='load_time_dimension_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="temps",
    append=False
)

load_enseigne_dimension_table = BuildDimensionOperator(
    task_id='load_enseigne_dimension_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="enseigne",
    append=False
)

load_famille_produit_dimension_table = BuildDimensionOperator(
    task_id='load_famille_produit_dimension_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="famille_produit",
    append=False
)

Load_sales_fact_table = LoadFactOperator(
    task_id='Load_sales_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="sales",
    query=SqlQueries.sales_table_insert
)





# run_quality_checks = DataQualityOperator(
#     task_id='run_quality_checks',
#     dag=dag,
#     redshift_conn_id="redshift",
#     check_null_sql=default_args['check_null_sql'],
#     check_count_sql=default_args['check_count_sql'],
#     tables=['songplays', 'songs', 'users', 'artists', 'time'],
#     columns=['playid', 'songid', 'userid', 'artistid', 'start_time']
# )

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)





#start_operator >> create_tables_task >> \
#[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table >> \
#[load_song_dimension_table, load_user_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> \
#run_quality_checks >> end_operator



start_operator >> create_tables >> \
[ 
    stage_CA_Fours_to_redshift, stage_MB_Fours_to_redshift, stage_V_Fours_to_redshift,
    stage_CA_Hifi_to_redshift, stage_MB_Hifi_to_redshift, stage_V_Hifi_to_redshift, 
    stage_CA_Magneto_to_redshift, stage_MB_Magneto_to_redshift, stage_V_Magneto_to_redshift,
    stage_currency_to_redshift, stage_cities_to_redshift
] >> \
milestone_1 >> \
[load_time_dimension_table, load_enseigne_dimension_table, load_famille_produit_dimension_table] >> \
milestone_2 >> \
Load_sales_fact_table >> \
end_operator

