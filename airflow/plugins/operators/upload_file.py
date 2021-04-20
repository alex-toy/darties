from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

import logging
import boto3
import os
from botocore.exceptions import ClientError


class UploadFileOperator(BaseOperator) :

    @apply_defaults
    def __init__(self,
                 aws_credentials_id="",
                 S3_bucket="",
                 S3_key="",
                 year=0,
                 file_name="",
                 path_to_file="",
                 *args, **kwargs):

        super(UploadFileOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.S3_bucket = S3_bucket
        self.S3_key = S3_key
        self.year = year
        self.file_name = file_name
        self.path_to_file = path_to_file
        


    def upload_file(self, aws_access_key_id, aws_secret_access_key, ACL={'ACL' : 'public-read'}):
        """
        Upload a file to an S3 bucket

        :param file_name: File to upload
        :param bucket: Bucket to upload to
        :param object_name: S3 object name. If not specified then file_name is used
        :return: True if file was uploaded, else False
        """
        object_name = os.path.join(self.S3_key, self.year, self.file_name)

        # Upload the file
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )
        try:
            response = s3_client.upload_file(self.file_name, self.S3_bucket, object_name, ExtraArgs=ACL)
        except ClientError as e:
            logging.error(e)
            return False
        return True


    def execute(self, context):
        credentials = AwsHook(self.aws_credentials_id).get_credentials()
        self.log.info('Uploading file.')
        self.upload_file(
            aws_access_key_id=credentials.access_key, 
            aws_secret_access_key=credentials.secret_key
        )




