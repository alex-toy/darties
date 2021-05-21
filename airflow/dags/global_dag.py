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



stage_to_redshifts = []
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

for kpi_item in kpi_items :
    stage_to_redshift = StageToRedshiftOperator(
        task_id=f"stage_{kpi_item['kpi_item']}_to_redshift",
        dag=dag,
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table=f"staging_{kpi_item['kpi_item']}",
        S3_bucket="darties",
        S3_key=kpi_item['kpi_item'],
        delimiter=",",
        formatting="JSON 'auto'"
    )
    stage_to_redshifts.append(stage_to_redshift)






# stage_CA_Fours_to_redshift = StageToRedshiftOperator(
#     task_id='stage_CA_Fours_to_redshift',
#     dag=dag,
#     redshift_conn_id="redshift",
#     aws_credentials_id="aws_credentials",
#     table="staging_CA_Fours",
#     S3_bucket="darties",
#     S3_key="CA_Fours",
#     delimiter=",",
#     formatting="JSON 'auto'"
# )

# stage_MB_Fours_to_redshift = StageToRedshiftOperator(
#     task_id='stage_MB_Fours_to_redshift',
#     dag=dag,
#     redshift_conn_id="redshift",
#     aws_credentials_id="aws_credentials",
#     table="staging_MB_Fours",
#     S3_bucket="darties",
#     S3_key="MB_Fours",
#     delimiter=",",
#     formatting="JSON 'auto'"
# )

# stage_V_Fours_to_redshift = StageToRedshiftOperator(
#     task_id='stage_V_Fours_to_redshift',
#     dag=dag,
#     redshift_conn_id="redshift",
#     aws_credentials_id="aws_credentials",
#     table="staging_V_Fours",
#     S3_bucket="darties",
#     S3_key="V_Fours",
#     delimiter=",",
#     formatting="JSON 'auto'"
# )


# stage_CA_Hifi_to_redshift = StageToRedshiftOperator(
#     task_id='stage_CA_Hifi_to_redshift',
#     dag=dag,
#     redshift_conn_id="redshift",
#     aws_credentials_id="aws_credentials",
#     table="staging_CA_Hifi",
#     S3_bucket="darties",
#     S3_key="CA_Hifi",
#     delimiter=",",
#     formatting="JSON 'auto'"
# )

# stage_MB_Hifi_to_redshift = StageToRedshiftOperator(
#     task_id='stage_MB_Hifi_to_redshift',
#     dag=dag,
#     redshift_conn_id="redshift",
#     aws_credentials_id="aws_credentials",
#     table="staging_MB_Hifi",
#     S3_bucket="darties",
#     S3_key="MB_Hifi",
#     delimiter=",",
#     formatting="JSON 'auto'"
# )

# stage_V_Hifi_to_redshift = StageToRedshiftOperator(
#     task_id='stage_V_Hifi_to_redshift',
#     dag=dag,
#     redshift_conn_id="redshift",
#     aws_credentials_id="aws_credentials",
#     table="staging_V_Hifi",
#     S3_bucket="darties",
#     S3_key="V_Hifi",
#     delimiter=",",
#     formatting="JSON 'auto'"
# )


# stage_CA_Magneto_to_redshift = StageToRedshiftOperator(
#     task_id='stage_CA_Magneto_to_redshift',
#     dag=dag,
#     redshift_conn_id="redshift",
#     aws_credentials_id="aws_credentials",
#     table="staging_CA_Magneto",
#     S3_bucket="darties",
#     S3_key="CA_Magneto",
#     delimiter=",",
#     formatting="JSON 'auto'"
# )

# stage_MB_Magneto_to_redshift = StageToRedshiftOperator(
#     task_id='stage_MB_Magneto_to_redshift',
#     dag=dag,
#     redshift_conn_id="redshift",
#     aws_credentials_id="aws_credentials",
#     table="staging_MB_Magneto",
#     S3_bucket="darties",
#     S3_key="MB_Magneto",
#     delimiter=",",
#     formatting="JSON 'auto'"
# )

# stage_V_Magneto_to_redshift = StageToRedshiftOperator(
#     task_id='stage_V_Magneto_to_redshift',
#     dag=dag,
#     redshift_conn_id="redshift",
#     aws_credentials_id="aws_credentials",
#     table="staging_V_Magneto",
#     S3_bucket="darties",
#     S3_key="V_Magneto",
#     delimiter=",",
#     formatting="JSON 'auto'"
# )

# stage_cities_to_redshift = StageToRedshiftOperator(
#     task_id='stage_cities_to_redshift',
#     dag=dag,
#     redshift_conn_id="redshift",
#     aws_credentials_id="aws_credentials",
#     table="staging_cities",
#     S3_bucket="darties",
#     S3_key="ville",
#     delimiter=",",
#     formatting="JSON 'auto'"
# )

# stage_currency_to_redshift = StageToRedshiftOperator(
#     task_id='stage_currency_to_redshift',
#     dag=dag,
#     redshift_conn_id="redshift",
#     aws_credentials_id="aws_credentials",
#     table="staging_currency",
#     S3_bucket="darties",
#     S3_key="currency",
#     delimiter=",",
#     formatting="JSON 'auto'"
# )

# stage_mapping_to_redshift = StageToRedshiftOperator(
#     task_id='stage_mapping_to_redshift',
#     dag=dag,
#     redshift_conn_id="redshift",
#     aws_credentials_id="aws_credentials",
#     table="staging_mapping",
#     S3_bucket="darties",
#     S3_key="mapping",
#     delimiter=",",
#     formatting="JSON 'auto'"
# )

# stage_utilisateur_to_redshift = StageToRedshiftOperator(
#     task_id='stage_utilisateur_to_redshift',
#     dag=dag,
#     redshift_conn_id="redshift",
#     aws_credentials_id="aws_credentials",
#     table="staging_utilisateur",
#     S3_bucket="darties",
#     S3_key="users",
#     delimiter=",",
#     formatting="JSON 'auto'"
# )

# stage_profil_to_redshift = StageToRedshiftOperator(
#     task_id='stage_profil_to_redshift',
#     dag=dag,
#     redshift_conn_id="redshift",
#     aws_credentials_id="aws_credentials",
#     table="staging_profil",
#     S3_bucket="darties",
#     S3_key="profil",
#     delimiter=",",
#     formatting="JSON 'auto'"
# )

# stage_enseigne_to_redshift = StageToRedshiftOperator(
#     task_id='stage_enseigne_to_redshift',
#     dag=dag,
#     redshift_conn_id="redshift",
#     aws_credentials_id="aws_credentials",
#     table="staging_enseigne",
#     S3_bucket="darties",
#     S3_key="enseignes",
#     delimiter=",",
#     formatting="JSON 'auto'"
# )

# stage_magasin_to_redshift = StageToRedshiftOperator(
#     task_id='stage_magasin_to_redshift',
#     dag=dag,
#     redshift_conn_id="redshift",
#     aws_credentials_id="aws_credentials",
#     table="staging_magasin",
#     S3_bucket="darties",
#     S3_key="magasin",
#     delimiter=",",
#     formatting="JSON 'auto'"
# )



#Build dimensions
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




# load_time_dimension_table = BuildDimensionOperator(
#     task_id='load_time_dimension_table',
#     dag=dag,
#     redshift_conn_id="redshift",
#     table="temps",
#     append=False
# )


# load_famille_produit_dimension_table = BuildDimensionOperator(
#     task_id='load_famille_produit_dimension_table',
#     dag=dag,
#     redshift_conn_id="redshift",
#     table="famille_produit",
#     append=False
# )



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





# load_ville_dimension_table = LoadDimensionOperator(
#     task_id='load_ville_dimension_table',
#     dag=dag,
#     redshift_conn_id="redshift",
#     table="ville",
#     query=SqlQueries.ville_table_insert,
#     append=False
# )


# load_devise_dimension_table = LoadDimensionOperator(
#     task_id='load_devise_dimension_table',
#     dag=dag,
#     redshift_conn_id="redshift",
#     table="devise",
#     query=SqlQueries.devise_table_insert,
#     append=False
# )


# load_cours_dimension_table = LoadDimensionOperator(
#     task_id='load_cours_dimension_table',
#     dag=dag,
#     redshift_conn_id="redshift",
#     table="cours",
#     query=SqlQueries.cours_table_insert,
#     append=False
# )


# load_magasin_dimension_table = LoadDimensionOperator(
#     task_id='load_magasin_dimension_table',
#     dag=dag,
#     redshift_conn_id="redshift",
#     table="magasin",
#     query=SqlQueries.magasin_table_insert,
#     append=False
# )


#Build fact table
milestone_2 = DummyOperator(task_id='milestone_2',  dag=dag)

Load_sales_fact_table = LoadFactOperator(
    task_id='Load_sales_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="sales",
    query=SqlQueries.sales_table_insert
)

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



unstage_ville_to_S3 = UnstageFromRedshiftOperator(
    task_id='unstage_ville_to_S3',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    S3_bucket="darties",
    table="ville"
)

unstage_temps_to_S3 = UnstageFromRedshiftOperator(
    task_id='unstage_temps_to_S3',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    S3_bucket="darties",
    table="temps"
)

# unstage_magasin_to_S3 = UnstageFromRedshiftOperator(
#     task_id='unstage_magasin_to_S3',
#     dag=dag,
#     redshift_conn_id="redshift",
#     aws_credentials_id="aws_credentials",
#     S3_bucket="darties",
#     table="magasin"
# )

# unstage_cours_to_S3 = UnstageFromRedshiftOperator(
#     task_id='unstage_cours_to_S3',
#     dag=dag,
#     redshift_conn_id="redshift",
#     aws_credentials_id="aws_credentials",
#     S3_bucket="darties",
#     table="cours"
# )

# unstage_devise_to_S3 = UnstageFromRedshiftOperator(
#     task_id='unstage_devise_to_S3',
#     dag=dag,
#     redshift_conn_id="redshift",
#     aws_credentials_id="aws_credentials",
#     S3_bucket="darties",
#     table="devise"
# )

# unstage_famille_produit_to_S3 = UnstageFromRedshiftOperator(
#     task_id='unstage_famille_produit_to_S3',
#     dag=dag,
#     redshift_conn_id="redshift",
#     aws_credentials_id="aws_credentials",
#     S3_bucket="darties",
#     table="famille_produit"
# )

# unstage_enseigne_to_S3 = UnstageFromRedshiftOperator(
#     task_id='unstage_enseigne_to_S3',
#     dag=dag,
#     redshift_conn_id="redshift",
#     aws_credentials_id="aws_credentials",
#     S3_bucket="darties",
#     table="staging_enseigne"
# )

# unstage_utilisateur_to_S3 = UnstageFromRedshiftOperator(
#     task_id='unstage_utilisateur_to_S3',
#     dag=dag,
#     redshift_conn_id="redshift",
#     aws_credentials_id="aws_credentials",
#     S3_bucket="darties",
#     table="utilisateur"
# )

# unstage_profil_to_S3 = UnstageFromRedshiftOperator(
#     task_id='unstage_profil_to_S3',
#     dag=dag,
#     redshift_conn_id="redshift",
#     aws_credentials_id="aws_credentials",
#     S3_bucket="darties",
#     table="staging_profil"
# )



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



# start_operator >> create_tables >> \
# [ 
#     stage_CA_Fours_to_redshift, stage_MB_Fours_to_redshift, stage_V_Fours_to_redshift,
#     stage_CA_Hifi_to_redshift, stage_MB_Hifi_to_redshift, stage_V_Hifi_to_redshift, 
#     stage_CA_Magneto_to_redshift, stage_MB_Magneto_to_redshift, stage_V_Magneto_to_redshift,
#     stage_currency_to_redshift, stage_cities_to_redshift, stage_mapping_to_redshift,
#     stage_utilisateur_to_redshift, stage_profil_to_redshift, stage_enseigne_to_redshift,
#     stage_magasin_to_redshift
# ] >> \
# milestone_1 >> \
# [
#     load_time_dimension_table, load_famille_produit_dimension_table, load_ville_dimension_table, 
#     load_devise_dimension_table, load_cours_dimension_table, load_magasin_dimension_table
# ] >> \
# Load_sales_fact_table >> \
# [
#     null_quality_checks, positive_quality_checks
# ] >> \
# milestone_2 >> \
# [
#     unstage_temps_to_S3, unstage_ville_to_S3, unstage_magasin_to_S3, unstage_cours_to_S3, 
#     unstage_devise_to_S3, unstage_famille_produit_to_S3, unstage_enseigne_to_S3, 
#     unstage_utilisateur_to_S3, unstage_profil_to_S3
# ] >> \
# end_operator

