from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class UnstageFromRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("S3_key", )

    unload_sql = """
        UNLOAD ('SELECT * FROM {}')
        TO 's3://{}/dataviz/{}_' 
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        parallel off;
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 S3_bucket=""
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.S3_bucket = S3_bucket


    def execute(self, context):
        credentials = AwsHook(self.aws_credentials_id).get_credentials()
        redshift = PostgresHook(self.redshift_conn_id)

        self.log.info('Copying data from Redshift into S3.')
        
        formatted_sql = UnstageFromRedshiftOperator.unload_sql.format(
            self.table,
            self.S3_bucket,
            self.table,
            credentials.access_key,
            credentials.secret_key,
        )
        self.log.info(f"formatted_sql : {formatted_sql}")
        
        redshift.run(formatted_sql)







