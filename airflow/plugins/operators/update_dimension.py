from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class UpdateDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 update_query="",
                 kpi="",
                 staging_monthly_table="",
                 *args, **kwargs):

        super(UpdateDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.update_query = update_query
        self.kpi = kpi
        self.staging_monthly_table = staging_monthly_table
        

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        
        sql_statement = self.update_query.format(self.kpi, self.staging_monthly_table)
                            #update_query.format('CA_reel', 'staging_monthly_ca_fours')


        self.log.info(f"sql_statement : {self.sql_statement}")

    

        redshift.run(sql_statement)
        
        self.log.info(f"Ending UpdateDimensionOperator {self.table} with update_query : {self.update_query}")

        
        

        
        
        