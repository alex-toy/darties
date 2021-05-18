from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class UpdateDimensionOperator(BaseOperator):

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
                 *args, **kwargs):

        super(UpdateDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.query = query
        self.append = append

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        
        sql_statement = UpdateDimensionOperator.table_insert.format(self.table, self.query)
        redshift.run(sql_statement)
        
        self.log.info(f"Ending UpdateDimensionOperator {self.table} with a Success on Operation  {operation}")

        
        

        
        
        