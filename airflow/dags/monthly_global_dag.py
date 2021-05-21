from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

from operators.stage_redshift import StageToRedshiftOperator
from operators.unstage_from_redshift import UnstageFromRedshiftOperator

from operators.check_monthly_null import CheckMonthlyNullOperator

from operators.load_fact import LoadFactOperator
from operators.update_dimension import UpdateDimensionOperator

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

# DAG
dag = DAG(
    'monthly_global_dag',
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
    sql='create_monthly_tables.sql',
    postgres_conn_id="redshift"
)

stage_monthly_to_redshifts = []
kpi_items = [
    {'kpi_item' : 'ca_fours', 'S3_key' : 'monthly_CA_Fours'}, 
    {'kpi_item' : 'v_fours', 'S3_key' : 'monthly_V_Fours'}, 
    {'kpi_item' : 'mb_fours', 'S3_key' : 'monthly_MB_Fours'},
    {'kpi_item' : 'ca_hifi', 'S3_key' : 'monthly_CA_Hifi'}, 
    {'kpi_item' : 'v_hifi', 'S3_key' : 'monthly_V_Hifi'}, 
    {'kpi_item' : 'mb_hifi', 'S3_key' : 'monthly_MB_Hifi'},
    {'kpi_item' : 'ca_magneto', 'S3_key' : 'monthly_CA_Magneto'}, 
    {'kpi_item' : 'v_magneto', 'S3_key' : 'monthly_V_Magneto'}, 
    {'kpi_item' : 'mb_magneto', 'S3_key' : 'monthly_MB_Magneto'}
]

for kpi_item in kpi_items :
    stage_monthly_to_redshift = StageToRedshiftOperator(
        task_id=f"stage_monthly_{kpi_item['kpi_item']}_to_redshift",
        dag=dag,
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table=f"staging_monthly_{kpi_item['kpi_item']}",
        S3_bucket="darties",
        S3_key=f"{kpi_item['S3_key']}",
        delimiter=",",
        formatting="JSON 'auto'"
    )
    stage_monthly_to_redshifts.append(stage_monthly_to_redshift)




### Update sales table
milestone_1 = DummyOperator(task_id='milestone_1',  dag=dag)


update_tables = []
kpi_items = [
    {'kpi_item' : 'ca_fours', 'kpi' : 'CA_reel', 'item' : 'fours'}, 
    {'kpi_item' : 'v_fours', 'kpi' : 'vente_reel', 'item' : 'fours'}, 
    {'kpi_item' : 'mb_fours', 'kpi' : 'marge_reel', 'item' : 'fours'},
    {'kpi_item' : 'ca_hifi', 'kpi' : 'CA_reel', 'item' : 'hifi'}, 
    {'kpi_item' : 'v_hifi', 'kpi' : 'vente_reel', 'item' : 'hifi'}, 
    {'kpi_item' : 'mb_hifi', 'kpi' : 'marge_reel', 'item' : 'hifi'},
    {'kpi_item' : 'ca_magneto', 'kpi' : 'CA_reel', 'item' : 'magneto'}, 
    {'kpi_item' : 'v_magneto', 'kpi' : 'vente_reel', 'item' : 'magneto'}, 
    {'kpi_item' : 'mb_magneto', 'kpi' : 'marge_reel', 'item' : 'magneto'}
]

id_famille_produit = {
    'fours' : 3,
    'magneto' : 2,
    'hifi' : 1
}

for kpi_item in kpi_items :
    update_table = UpdateDimensionOperator(
        task_id=f"update_{kpi_item['kpi_item']}_table",
        dag=dag,
        redshift_conn_id="redshift",
        update_query=UpdateSqlQueries.update_query,
        kpi=f"{kpi_item['kpi']}",
        item=f"{kpi_item['item']}",
        id_famille_produit=id_famille_produit[kpi_item['item']],
        staging_monthly_table=f"staging_monthly_{kpi_item['kpi_item']}"
    )
    update_tables.append(update_table)





### Quality checks
milestone_2 = DummyOperator(task_id='milestone_2',  dag=dag)
columns = ['ca_reel', 'vente_reel', 'marge_reel']
months = ['janvier', 'fevrier', 'mars']
null_quality_checks = []

for month in months :
    null_quality_check = CheckMonthlyNullOperator(
        task_id=f"{month}_null_quality_checks",
        dag=dag,
        redshift_conn_id="redshift",
        columns=columns,
        month=month
    )
    null_quality_checks.append(null_quality_check)




## Unstage to S3
unstage_sales_to_S3 = UnstageFromRedshiftOperator(
    task_id='unstage_sales_to_S3',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    S3_bucket="darties",
    table="sales"
)


end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


## DAG
start_operator >> create_tables >> \
[ 
    smr for smr in stage_monthly_to_redshifts
] >> \
milestone_1 >> \
[
    ut for ut in update_tables
] >> \
milestone_2 >> \
[
    nqc for nqc in null_quality_checks
] >> \
unstage_sales_to_S3 >> \
end_operator



