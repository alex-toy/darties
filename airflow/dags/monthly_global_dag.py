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

stage_monthly_ca_fours_to_redshift = StageToRedshiftOperator(
    task_id='stage_monthly_ca_fours_to_redshift',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_monthly_ca_fours",
    S3_bucket="darties",
    S3_key="monthly_CA_Fours",
    delimiter=",",
    formatting="JSON 'auto'"
)

stage_monthly_v_fours_to_redshift = StageToRedshiftOperator(
    task_id='stage_monthly_v_fours_to_redshift',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_monthly_v_fours",
    S3_bucket="darties",
    S3_key="monthly_V_Fours",
    delimiter=",",
    formatting="JSON 'auto'"
)

stage_monthly_mb_fours_to_redshift = StageToRedshiftOperator(
    task_id='stage_monthly_mb_fours_to_redshift',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_monthly_mb_fours",
    S3_bucket="darties",
    S3_key="monthly_MB_Fours",
    delimiter=",",
    formatting="JSON 'auto'"
)



stage_monthly_ca_hifi_to_redshift = StageToRedshiftOperator(
    task_id='stage_monthly_ca_hifi_to_redshift',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_monthly_ca_hifi",
    S3_bucket="darties",
    S3_key="monthly_CA_Hifi",
    delimiter=",",
    formatting="JSON 'auto'"
)

stage_monthly_v_hifi_to_redshift = StageToRedshiftOperator(
    task_id='stage_monthly_v_hifi_to_redshift',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_monthly_v_hifi",
    S3_bucket="darties",
    S3_key="monthly_V_Hifi",
    delimiter=",",
    formatting="JSON 'auto'"
)

stage_monthly_mb_hifi_to_redshift = StageToRedshiftOperator(
    task_id='stage_monthly_mb_hifi_to_redshift',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_monthly_mb_hifi",
    S3_bucket="darties",
    S3_key="monthly_MB_Hifi",
    delimiter=",",
    formatting="JSON 'auto'"
)



stage_monthly_ca_magneto_to_redshift = StageToRedshiftOperator(
    task_id='stage_monthly_ca_magneto_to_redshift',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_monthly_ca_magneto",
    S3_bucket="darties",
    S3_key="monthly_CA_Magneto",
    delimiter=",",
    formatting="JSON 'auto'"
)

stage_monthly_v_magneto_to_redshift = StageToRedshiftOperator(
    task_id='stage_monthly_v_magneto_to_redshift',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_monthly_v_magneto",
    S3_bucket="darties",
    S3_key="monthly_V_Magneto",
    delimiter=",",
    formatting="JSON 'auto'"
)

stage_monthly_mb_magneto_to_redshift = StageToRedshiftOperator(
    task_id='stage_monthly_mb_magneto_to_redshift',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_monthly_mb_magneto",
    S3_bucket="darties",
    S3_key="monthly_MB_Magneto",
    delimiter=",",
    formatting="JSON 'auto'"
)



### Update sales table
milestone_1 = DummyOperator(task_id='milestone_1',  dag=dag)

update_ca_fours_table = UpdateDimensionOperator(
    task_id='update_ca_fours_table',
    dag=dag,
    redshift_conn_id="redshift",
    update_query=UpdateSqlQueries.update_query,
    kpi="CA_reel",
    item="fours",
    id_famille_produit=3,
    staging_monthly_table="staging_monthly_ca_fours"
)

update_v_fours_table = UpdateDimensionOperator(
    task_id='update_v_fours_table',
    dag=dag,
    redshift_conn_id="redshift",
    update_query=UpdateSqlQueries.update_query,
    kpi="vente_reel",
    item="fours",
    id_famille_produit=3,
    staging_monthly_table="staging_monthly_v_fours"
)

update_mb_fours_table = UpdateDimensionOperator(
    task_id='update_mb_fours_table',
    dag=dag,
    redshift_conn_id="redshift",
    update_query=UpdateSqlQueries.update_query,
    kpi="marge_reel",
    item="fours",
    id_famille_produit=3,
    staging_monthly_table="staging_monthly_mb_fours"
)


update_ca_hifi_table = UpdateDimensionOperator(
    task_id='update_ca_hifi_table',
    dag=dag,
    redshift_conn_id="redshift",
    update_query=UpdateSqlQueries.update_query,
    kpi="CA_reel",
    item="hifi",
    id_famille_produit=1,
    staging_monthly_table="staging_monthly_ca_hifi"
)

update_v_hifi_table = UpdateDimensionOperator(
    task_id='update_v_hifi_table',
    dag=dag,
    redshift_conn_id="redshift",
    update_query=UpdateSqlQueries.update_query,
    kpi="vente_reel",
    item="hifi",
    id_famille_produit=1,
    staging_monthly_table="staging_monthly_v_hifi"
)

update_mb_hifi_table = UpdateDimensionOperator(
    task_id='update_mb_hifi_table',
    dag=dag,
    redshift_conn_id="redshift",
    update_query=UpdateSqlQueries.update_query,
    kpi="marge_reel",
    item="hifi",
    id_famille_produit=1,
    staging_monthly_table="staging_monthly_mb_hifi"
)


update_ca_magneto_table = UpdateDimensionOperator(
    task_id='update_ca_magneto_table',
    dag=dag,
    redshift_conn_id="redshift",
    update_query=UpdateSqlQueries.update_query,
    kpi="CA_reel",
    item="magneto",
    id_famille_produit=2,
    staging_monthly_table="staging_monthly_ca_magneto"
)

update_v_magneto_table = UpdateDimensionOperator(
    task_id='update_v_magneto_table',
    dag=dag,
    redshift_conn_id="redshift",
    update_query=UpdateSqlQueries.update_query,
    kpi="vente_reel",
    item="magneto",
    id_famille_produit=2,
    staging_monthly_table="staging_monthly_v_magneto"
)

update_mb_magneto_table = UpdateDimensionOperator(
    task_id='update_mb_magneto_table',
    dag=dag,
    redshift_conn_id="redshift",
    update_query=UpdateSqlQueries.update_query,
    kpi="marge_reel",
    item="magneto",
    id_famille_produit=2,
    staging_monthly_table="staging_monthly_mb_magneto"
)


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


# january_null_quality_checks = CheckMonthlyNullOperator(
#     task_id='january_null_quality_checks',
#     dag=dag,
#     redshift_conn_id="redshift",
#     columns=columns,
#     month='janvier'
# )

# february_null_quality_checks = CheckMonthlyNullOperator(
#     task_id='february_null_quality_checks',
#     dag=dag,
#     redshift_conn_id="redshift",
#     columns=columns,
#     month='fevrier'
# )



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
    stage_monthly_ca_fours_to_redshift, stage_monthly_v_fours_to_redshift, stage_monthly_mb_fours_to_redshift,
    stage_monthly_ca_hifi_to_redshift, stage_monthly_v_hifi_to_redshift, stage_monthly_mb_hifi_to_redshift,
    stage_monthly_ca_magneto_to_redshift, stage_monthly_v_magneto_to_redshift, stage_monthly_mb_magneto_to_redshift
] >> \
milestone_1 >> \
[
    update_ca_fours_table, update_v_fours_table, update_mb_fours_table,
    update_ca_hifi_table, update_v_hifi_table, update_mb_hifi_table,
    update_ca_magneto_table, update_v_magneto_table, update_mb_magneto_table
] >> \
milestone_2 >> \
[
    null_quality_check for null_quality_check in null_quality_checks
] >> \
unstage_sales_to_S3 >> \
end_operator



