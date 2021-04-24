from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

import os
import config.config as cf
from os import listdir
from os.path import isfile, join


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
            file_abspath = os.path.abspath(os.path.join(dirpath, file))
            self.log.info(f"file_abspath : {file_abspath}")
        
        
            #sd = SalesData(path=cf.FILE_DATA)
            #sd.cleaned_file(year=self.year)
            
        
        
    
    
    
