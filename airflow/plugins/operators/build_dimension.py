import math
import calendar

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
                 append=False,
                 *args, **kwargs):

        super(BuildDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.append = append



    def build_time_query(self) :
        time_table_insert = ("""(annee, semestre, trimestre, mois, lib_mois) VALUES""")

        for year in range(2015, 2025) :
            for num_month in range(1,13):
                trimestre = 1 + num_month // 3
                semestre = 1 + num_month // 6
                month = cf.NUM_TO_NAME_MONTH[num_month]
                time_table_insert += f" ({year}, {semestre}, {trimestre}, {num_month}, {month}),"

        time_table_insert = time_table_insert[:-1] + ';'
        return time_table_insert

        

    def build_famille_produit_query(self) :
        famille_produit_table_insert = """
            (id_famille_produit, lib_famille_produit) VALUES
            (1, 'hifi'),
            (2, 'magneto'),
            (3, 'fours');
        """
        return famille_produit_table_insert



    def execute(self, context):
        if self.table == 'temps' :
            query = self.build_time_query()
        elif self.table == 'famille_produit' :
            query = self.build_famille_produit_query()

        self.log.info(f"query : {query}")

        redshift = PostgresHook(self.redshift_conn_id)
        if self.append == False :
            sql_statement = BuildDimensionOperator.truncate_sql.format(self.table)
            sql_statement += BuildDimensionOperator.table_insert.format(self.table, query)
            operation = 'truncate'
        else:
            sql_statement = BuildDimensionOperator.table_insert.format(self.table, query)
            operation = 'append'
            
        redshift.run(sql_statement)
        
        self.log.info(f"Ending BuildDimensionOperator {self.table} with a Success on Operation  {operation}")

        
        

        
        
        