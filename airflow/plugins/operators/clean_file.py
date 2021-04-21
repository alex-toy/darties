from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

import os

from infrastructure.SalesData import SalesData



class CleanFileOperator(BaseOperator) :

    @apply_defaults
    def __init__(self,
                 repo_dir="",
                 file_name="",
                 year="",
                 *args, **kwargs):

        super(CleanFileOperator, self).__init__(*args, **kwargs)
        self.repo_dir = repo_dir
        self.file_name = file_name
        self.year = year

        self.output_dir = os.path.join(os.path.dirname(__file__), '../output', self.year)
        self.path_to_data = os.path.join(self.repo_dir, self.file_name)


    def execute(self, context):
        self.log.info(f"Retrieving file at  : {self.path_to_data}.")
        df = SalesData.df_from_path(path=self.path_to_data)
        sd = SalesData(df)
        
        cleaned_file_name = 'cleaned_' + str(self.year) + '_sales.json'
        self.log.info(f"Creating file {cleaned_file_name} before upload to S3.")
        self.log.info(f"Year : {self.year}.")
        sd.cleaned_file(self.year, cleaned_file_name)
        
        
        
    

