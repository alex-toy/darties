from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

import os
import config.config as cf
from os import listdir
from os.path import isfile, join
import re




class CleanMonthlyFileOperator(BaseOperator) :

    @apply_defaults
    def __init__(self,
                item='',
                UtilityClass=None,
                 *args, **kwargs):
        super(CleanMonthlyFileOperator, self).__init__(*args, **kwargs)
        self.item = item
        self.UtilityClass = UtilityClass



    def execute(self, context):
        item_path = os.path.abspath(os.path.join(cf.DATA_DIR, self.item))
        self.log.info(f"Create cleaned files in : {item_path}")
        files = [f for f in listdir(item_path) if isfile(join(item_path, f))]
        for file in files :
            if file != 'README.md' and not file.startswith('.') :
                file_abspath = os.path.abspath(os.path.join(cf.DATA_DIR, self.item, file))
                self.log.info(f"Name of files : {file}")
                year = re.search(r'(\d{4})', file).group(1)
                month = re.search(r'(\w+)_', file).group(1).lower()
                self.log.info(f"Create cleaned file : {file} for year {year} and month {month}")
                sd = self.UtilityClass(path=file_abspath)
                sd.cleaned_file(year=year, month=month)
            
        
        
    
    
    
