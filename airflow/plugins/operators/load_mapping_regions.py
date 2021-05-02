from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

import os
import config.config as cf
from os import listdir
from os.path import isfile, join
import re
import requests
from bs4 import BeautifulSoup
import datetime
from datetime import datetime, timedelta, date
from pathlib import Path
import pandas as pd



class LoadMappingOperator(BaseOperator) :

    @apply_defaults
    def __init__(self,
                 *args, **kwargs):
        super(LoadMappingOperator, self).__init__(*args, **kwargs)




    def get_data_from(self, url):
        r = requests.get(url)
        bs = BeautifulSoup(r.text, 'lxml')
        trs = bs.find_all("tr")[1:23]

        previous_names = []
        new_names = []

        for tr in trs :
            tds = tr.find_all('td')
            if len(tds) == 2 :
                new_name = tds[0].text
                prev_name = tds[1].text
            else :
                new_name = new_names[-1]
                prev_name = tds[0].text

            new_names.append(new_name)
            previous_names.append(prev_name)
            
        return previous_names, new_names



    def get_data_with_previous_names(self, url):
        r = requests.get(url)
        bs = BeautifulSoup(r.text, 'lxml')
        trs = bs.find_all("tr")
        #self.log.info(f"trs : {trs}")
        filtered_trs = [ tr for tr in trs if len(tr)==9 ]

        for tr in filtered_trs :
            self.log.info(f"len tr : {len(tr)}")
            self.log.info(f"tr : {tr}")

        departements = []
        regions = []

        return departements, regions




    def create_mapping_file(self, previous_names, new_names) :
        data = list(zip(previous_names, new_names))
        df = pd.DataFrame(data, columns =['previous_names', 'new_names'])
        df = cf.remove_accents(df=df)

        now = datetime.now()
        outdir = os.path.join(cf.OUTPUTS_DIR, 'mapping', str(now.year))
        path = Path(outdir)
        path.mkdir(parents=True, exist_ok=True)
        saved_filename = f"mapping_{str(now.year)}.json"
        df.to_json(os.path.join(outdir, saved_filename), orient="records", lines=True)





    def execute(self, context):
        # self.log.info(f"load mapping from  : {cf.MAPPING_PREV_NEW_REG}")
        # previous_names, new_names = self.get_data_from(cf.MAPPING_PREV_NEW_REG)
        
        # self.log.info(f"create csv file from cities.")
        # self.create_mapping_file(previous_names, new_names)

        self.log.info(f"load mapping from  : {cf.MAPPING_DEP_PREV_REG}")
        departements, regions = self.get_data_with_previous_names(cf.MAPPING_DEP_PREV_REG)
        
        #self.log.info(f"create csv file from cities.")
        #self.create_mapping_file(previous_names, new_names)
        
        
    
    
