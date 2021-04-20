from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

import logging
import boto3
import os
from botocore.exceptions import ClientError

from infrastructure.SalesData import SalesData


class CleanFileOperator(BaseOperator) :

    @apply_defaults
    def __init__(self,
                 path_to_data="",
                 repo_dir="",
                 file_name="",
                 year="",
                 output_dir="", 
                 saved_filename="",
                 *args, **kwargs):

        super(UploadFileOperator, self).__init__(*args, **kwargs)
        self.repo_dir=repo_dir,
        self.file_name=file_name,
        self.year=year
        self.output_dir=output_dir
        
        self.path_to_data = os.path.join(self.repo_dir, self.year, self.file_name)


    def execute(self, context):
        df = SalesData.df_from_path(path=self.path_to_data)
        sd = SalesData(df)
        
        self.log.info(f"Creating file {self.path} to S3.")
        sd.cleaned_file(self.year, self.output_dir, self.saved_filename)
        
        
        
    

