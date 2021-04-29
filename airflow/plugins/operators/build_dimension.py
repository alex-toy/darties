import locale
locale.setlocale(locale.LC_ALL, 'fr_FR')
import math

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
        self.query = query
        self.append = append

    def build_time_query(self) :
        time_table_insert = ("""(annee, semestre, trimestre, mois, lib_mois)""")

        for year in range(2015, 2025) :
            for num_month in range(1,13):
                trimestre = num_month // 3
                semestre = num_month // 6
                month = calendar.month_name[num_month].replace("û", "u")
                time_table_insert += f" VALUES({year}, {semestre}, {trimestre}, {num_month}, {month}),"

        time_table_insert[-1] = ';'

        return time_table_insert


    def build_enseigne_query(self) :
        enseigne_table_insert = ("""(id_enseigne, lib_enseigne)""")
    	time_table_insert += " VALUES(1, lib_enseigne),"

        




    def execute(self, context):
        if self.table == 'temps' :
            query = self.build_time_query()
        
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

        
        

        
        
        