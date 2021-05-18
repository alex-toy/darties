from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class UpdateDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    update_query = """
        INSERT INTO {} {}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 query="",
                 *args, **kwargs):

        super(UpdateDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.query = query

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        
        sql_statement = UpdateDimensionOperator.update_query.format(self.table, self.query)
        redshift.run(sql_statement)
        
        self.log.info(f"Ending UpdateDimensionOperator {self.table} with a Success on Operation  {operation}")

        
        

        
        
        