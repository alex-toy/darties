from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

import os
import config.config as cf
from os import listdir
from os.path import isfile, join
import re


from infrastructure.SalesData import SalesData


class CleanFileOperator(BaseOperator) :

    @apply_defaults
    def __init__(self,
                 year="",
                 *args, **kwargs):

        super(CleanFileOperator, self).__init__(*args, **kwargs)
        self.year = year



    def execute(self, context):
        self.log.info(f"Create cleaned files in : {cf.DATA_DIR}")
        files = [f for f in listdir(cf.DATA_DIR) if isfile(join(cf.DATA_DIR, f))]
        for file in files :
            if file != 'README.md' :
                file_abspath = os.path.abspath(os.path.join(cf.DATA_DIR, file))
                year = re.search(r'(\d{4})', file).group(1)
                self.log.info(f"Create cleaned file : {file} for year {year}")
                sd = SalesData(path=file_abspath)
                sd.cleaned_file(year=year)
            
        
        
    
    
    
