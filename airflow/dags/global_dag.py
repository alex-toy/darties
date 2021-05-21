from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

from operators.stage_redshift import StageToRedshiftOperator
from operators.unstage_from_redshift import UnstageFromRedshiftOperator

from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.build_dimension import BuildDimensionOperator
from operators.check_null import CheckNullOperator
from operators.check_positive import CheckPositiveOperator

from helpers import SqlQueries



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


### Stage to redshift

stage_to_redshifts = []
kpi_items = [
    {'kpi_item' : 'ca_fours', 'kpi' : 'CA_reel', 'item' : 'fours', 'S3_key' : 'CA_Fours'}, 
    {'kpi_item' : 'v_fours', 'kpi' : 'vente_reel', 'item' : 'fours', 'S3_key' : 'V_Fours'}, 
    {'kpi_item' : 'mb_fours', 'kpi' : 'marge_reel', 'item' : 'fours', 'S3_key' : 'MB_Fours'},
    {'kpi_item' : 'ca_hifi', 'kpi' : 'CA_reel', 'item' : 'hifi', 'S3_key' : 'CA_Hifi'}, 
    {'kpi_item' : 'v_hifi', 'kpi' : 'vente_reel', 'item' : 'hifi', 'S3_key' : 'V_Hifi'}, 
    {'kpi_item' : 'mb_hifi', 'kpi' : 'marge_reel', 'item' : 'hifi', 'S3_key' : 'MB_Hifi'},
    {'kpi_item' : 'ca_magneto', 'kpi' : 'CA_reel', 'item' : 'magneto', 'S3_key' : 'CA_Magneto'}, 
    {'kpi_item' : 'v_magneto', 'kpi' : 'vente_reel', 'item' : 'magneto', 'S3_key' : 'V_Magneto'}, 
    {'kpi_item' : 'mb_magneto', 'kpi' : 'marge_reel', 'item' : 'magneto', 'S3_key' : 'MB_Magneto'},

    {'kpi_item' : 'currency', 'S3_key' : 'currency'},
    {'kpi_item' : 'cities', 'S3_key' : 'ville'},
    {'kpi_item' : 'mapping', 'S3_key' : 'mapping'},
    {'kpi_item' : 'profil', 'S3_key' : 'profil'},
    {'kpi_item' : 'utilisateur', 'S3_key' : 'users'},
    {'kpi_item' : 'enseigne', 'S3_key' : 'enseignes'},
    {'kpi_item' : 'magasin', 'S3_key' : 'magasin'},
    {'kpi_item' : 'magasin', 'S3_key' : 'magasin'}
]

for kpi_item in kpi_items :
    stage_to_redshift = StageToRedshiftOperator(
        task_id=f"stage_{kpi_item['kpi_item']}_to_redshift",
        dag=dag,
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table=f"staging_{kpi_item['kpi_item']}",
        S3_bucket="darties",
        S3_key=kpi_item['S3_key'],
        delimiter=",",
        formatting="JSON 'auto'"
    )
    stage_to_redshifts.append(stage_to_redshift)





### Build and load dimensions
milestone_1 = DummyOperator(task_id='milestone_1',  dag=dag)


build_dimension_tables = []
dimension_items = [
    "temps",
    "famille_produit"
]


for dimension_item in dimension_items :
    build_dimension_table = BuildDimensionOperator(
        task_id=f"build_{dimension_item}_dimension_table",
        dag=dag,
        redshift_conn_id="redshift",
        table=dimension_item,
        append=False
    )
    build_dimension_tables.append(build_dimension_table)




load_dimension_tables = []
dimension_items = [
    {'item' : 'ville', "query" : SqlQueries.ville_table_insert},
    {'item' : "devise", "query" : SqlQueries.devise_table_insert},
    {'item' : "cours", "query" : SqlQueries.cours_table_insert},
    {'item' : "magasin", "query" : SqlQueries.magasin_table_insert}
]


for dimension_item in dimension_items :
    load_dimension_table = LoadDimensionOperator(
        task_id=f"load_{dimension_item['item']}_dimension_table",
        dag=dag,
        redshift_conn_id="redshift",
        table=dimension_item["item"],
        query=dimension_item["query"],
        append=False
    )
    load_dimension_tables.append(load_dimension_table)






### Build fact table
milestone_2 = DummyOperator(task_id='milestone_2',  dag=dag)

Load_sales_fact_table = LoadFactOperator(
    task_id='Load_sales_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="sales",
    query=SqlQueries.sales_table_insert
)



### Quality checks
tables = ['sales', 'sales', 'sales', 'sales', 'magasin', 'utilisateur', 'cours']
columns = ['id_ville', 'id_temps', 'id_famille_produit', 'id_magasin', 'id_enseigne', 'id_profil', 'id_devise']

null_quality_checks = CheckNullOperator(
    task_id='null_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=tables,
    columns=columns
)

tables = ['sales', 'sales']
columns = ['vente_objectif', 'vente_reel']

positive_quality_checks = CheckPositiveOperator(
    task_id='positive_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=tables,
    columns=columns
)


## Unstage to S3

unstage_to_S3s = []
dimension_items = [
    "ville",
    "temps",
    "magasin",
    "cours",
    "devise",
    "famille_produit",
    "staging_enseigne",
    "utilisateur",
    "staging_profil"
]


for dimension_item in dimension_items :
    unstage_to_S3 = UnstageFromRedshiftOperator(
        task_id=f"unstage_{dimension_item}_to_S3",
        dag=dag,
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        S3_bucket="darties",
        table=dimension_item
    )
    unstage_to_S3s.append(unstage_to_S3)





end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)




## DAG

start_operator >> create_tables >> \
[ 
    std for std in stage_to_redshifts
] >> \
milestone_1 >> \
[
    dt for dt in load_dimension_tables + build_dimension_tables
] >> \
Load_sales_fact_table >> \
[
    null_quality_checks, positive_quality_checks
] >> \
milestone_2 >> \
[
    utS for utS in unstage_to_S3s
] >> \
end_operator




