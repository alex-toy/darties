from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

import config.config as cf

class BuildDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    table_insert = """
        INSERT INTO {} {}
    """
    truncate_sql = """
        TRUNCATE TABLE {};
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 query="",
                 append=False,
                 *args, **kwargs):

        super(BuildDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.query = query
        self.append = append

    def build_time_query() :
        time_table_insert = ("""(annee, semestre, trimestre, mois, lib_mois)""")


        for year in range(2015, 2025) :
            for month in cf.MONTHS :
            time_table_insert += """VALUES(2020,         1,         1,    1, 'janvier'),"""


            time_table_insert = ("""
                    (annee, semestre, trimestre, mois, lib_mois)
                VALUES(2020,         1,         1,    1, 'janvier'),
                VALUES(annee, semestre, trimestre, mois, lib_mois),
                VALUES(annee, semestre, trimestre, mois, lib_mois);
            """)

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        if self.append == False :
            sql_statement = BuildDimensionOperator.truncate_sql.format(self.table)
            sql_statement += BuildDimensionOperator.table_insert.format(self.table, self.query)
            operation = 'truncate'
        else:
            sql_statement = BuildDimensionOperator.table_insert.format(self.table, self.query)
            operation = 'append'
            
        redshift.run(sql_statement)
        
        self.log.info(f"Ending BuildDimensionOperator {self.table} with a Success on Operation  {operation}")

        
        

        
        
        