from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CheckPositiveOperator(BaseOperator):

    ui_color = '#89DA59'
    check_null_sql = """SELECT COUNT(*) FROM {} WHERE {} < 0;"""

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables=[],
                 columns=[],
                 *args, **kwargs):

        super(CheckPositiveOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        self.columns = columns

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        
        for table, column in zip(self.tables, self.columns) :
            check_query = CheckPositiveOperator.check_null_sql.format(table, column)
            records = redshift.get_records(check_query)[0]
            error_count = 0
            failing_tests = []
            if records[0] != 0 :
                error_count += 1
                failing_tests.append(check_query)

        if error_count > 0:
            self.log.info('SQL Tests failed')
            self.log.info(failing_tests)
            raise ValueError('Data quality check failed')
        else :
            self.log.info('SQL positive Tests Passed')
            
        
            

        
        
        
        