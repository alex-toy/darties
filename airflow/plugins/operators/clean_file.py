from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

import os
import config.config as cf
from os import listdir
from os.path import isfile, join
import re




class CleanFileOperator(BaseOperator) :

    @apply_defaults
    def __init__(self,
                type='',
                UtilityClass=None,
                 *args, **kwargs):
        super(CleanFileOperator, self).__init__(*args, **kwargs)



    def execute(self, context):
        self.log.info(f"Create cleaned files in : {cf.DATA_DIR}")
        type_path = os.path.abspath(os.path.join(cf.DATA_DIR, type))
        files = [f for f in listdir(type_path) if isfile(join(type_path, f))]
        for file in files :
            if file != 'README.md' and not file.startswith('.') :
                file_abspath = os.path.abspath(os.path.join(cf.DATA_DIR, type, file))
                self.log.info(f"Name of files : {file}")
                year = re.search(r'(\d{4})', file).group(1)
                self.log.info(f"Create cleaned file : {file} for year {year}")
                sd = UtilityClass(path=file_abspath)
                sd.cleaned_file(year=year)
            
        
        
    
    
    
