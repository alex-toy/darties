from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CheckMonthlyNullOperator(BaseOperator):

    ui_color = '#89DA59'
    check_null_sql = """
        SELECT COUNT(*) 
        FROM sales 
        JOIN temps ON temps.id_temps = sales.id_temps
        WHERE {} IS NULL AND temps.lib_mois = '{}';
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 columns=[],
                 month='janvier',
                 *args, **kwargs):

        super(CheckMonthlyNullOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.columns = columns
        self.month = month

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        
        for column in self.columns :
            check_query = CheckMonthlyNullOperator.check_null_sql.format(column, self.month)
            records = redshift.get_records(check_query)[0]
            error_count = 0
            failing_tests = []
            if records[0] != 0 :
                error_count += 1
                failing_tests.append(check_query)

        if error_count > 0:
            self.log.info('SQL Tests failed')
            self.log.info(failing_tests)
            raise ValueError(f"Data quality check failed for {self.month}")
        else :
            self.log.info('SQL null Tests Passed')
            
        
            

        
        
        
        