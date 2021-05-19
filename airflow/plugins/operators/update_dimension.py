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
                 item="",
                 id_famille_produit=1,
                 *args, **kwargs):

        super(UpdateDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.update_query = update_query
        self.kpi = kpi
        self.staging_monthly_table = staging_monthly_table
        self.item = item
        self.id_famille_produit = id_famille_produit
        

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        
        sql_statement = self.update_query.format(
            self.kpi, 
            self.staging_monthly_table, 
            self.item,
            self.id_famille_produit
        )

        self.log.info(f"sql_statement : {sql_statement}")

    

        redshift.run(sql_statement)
        
        self.log.info(f"Ending UpdateDimensionOperator {self.kpi} with update_query : {self.update_query}")

        
        

        
        
        