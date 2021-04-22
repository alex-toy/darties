from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

import os
import config.config as cf

from infrastructure.SalesData import SalesData


class CleanFileOperator(BaseOperator) :

    @apply_defaults
    def __init__(self,
                 year="",
                 *args, **kwargs):

        super(CleanFileOperator, self).__init__(*args, **kwargs)
        self.year = year



    def execute(self, context):
        self.log.info(f"Create cleaned files in : {cf.OUTPUTS_DIR}")
        sd = SalesData(path=cf.FILE_DATA)
        sd.cleaned_file(year=self.year)
        
        
        
    
    
    
