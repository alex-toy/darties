from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("S3_key", )
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        {};
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 S3_bucket="",
                 S3_key="",
                 delimiter=",",
                 formatting="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.S3_bucket = S3_bucket
        self.S3_key = S3_key
        self.formatting = formatting


    def execute(self, context):
        credentials = AwsHook(self.aws_credentials_id).get_credentials()
        redshift = PostgresHook(self.redshift_conn_id)
        
        self.log.info('Clearing data from destination Redshift table.')
        redshift.run(f"DELETE FROM {self.table}")

        self.log.info('Copying data from S3 into Redshift.')
        
        rendered_key = self.S3_key.format(**context)
        self.log.info(f"rendered_key : {rendered_key}") # get rendered_key
        
        s3_path = f"s3://{self.S3_bucket}/{rendered_key}"
        self.log.info(f"s3_path : {s3_path}") # get year from s3_path
        
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.formatting
        )
        self.log.info(f"formatted_sql : {formatted_sql}")
        
        # run at last
        redshift.run(formatted_sql)







