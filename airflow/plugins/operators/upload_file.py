from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

import logging
import boto3
import os
import re
import glob
from botocore.exceptions import ClientError

import config.config as cf


class UploadFileOperator(BaseOperator) :

    @apply_defaults
    def __init__(self,
                 aws_credentials_id="",
                 S3_bucket="",
                 output_dir=None,
                 S3_key="",
                 year=0,
                 file_name="",
                 path_to_file="",
                 *args, **kwargs):

        super(UploadFileOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.S3_bucket = S3_bucket
        self.output_dir = output_dir
        self.S3_key = S3_key
        self.year = year
        self.file_name = file_name
        self.path_to_file = path_to_file
        


    def upload_file(self, aws_access_key_id, object_name, path_to_file, aws_secret_access_key, ACL={'ACL' : 'public-read'}):
        """
        Upload a file to an S3 bucket
        :param path_to_file: complete path to file to upload
        :param bucket: Bucket to upload to
        :param object_name: S3 object name. If not specified then file_name is used
        :return: True if file was uploaded, else False
        """
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )
        try:
            response = s3_client.upload_file(path_to_file, self.S3_bucket, object_name, ExtraArgs=ACL)
        except ClientError as e:
            logging.error(e)
            return False
        return True



    def execute(self, context):
        credentials = AwsHook(self.aws_credentials_id).get_credentials()
        
        self.log.info(f"output_dir : {cf.OUTPUTS_DIR}")

        all_files = glob.glob(self.output_dir + '/**/*.json', recursive=True)

        for path_to_file in all_files :
            
            short_path_to_file = path_to_file[len(cf.OUTPUTS_DIR)+1:]
            S3_key = re.search(r'(\w+)(\/\d)', short_path_to_file).group(1)
            year = re.search(r'(\d{4})', short_path_to_file).group(1)
            filename = re.search(r'\/(\w*\.json)', short_path_to_file).group(1)
            
            object_name = os.path.join(self.S3_key, self.year, self.filename)
            self.log.info(f"object_name : {object_name}")

            self.log.info(f"Uploading file {object_name} to S3.")
            self.upload_file(
                aws_access_key_id=credentials.access_key, 
                ws_secret_access_key=credentials.secret_key,
                object_name=object_name,
                path_to_file=path_to_file
            )
