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
from pandas.api.types import is_string_dtype



class LoadCitiesOperator(BaseOperator) :

    @apply_defaults
    def __init__(self,
                 *args, **kwargs):
        super(LoadCitiesOperator, self).__init__(*args, **kwargs)



    def get_data_from(self, url):
        r = requests.get(url)
        bs = BeautifulSoup(r.text, 'lxml')
        trs = bs.find_all('tr')
        
        filtered_trs = [tr for tr in trs if len(tr.find_all('td'))>=30]
        self.log.info(f"filtered_trs  : {filtered_trs} ")

        cities = []
        departements = []
        regions = []

        #for tr in filtered_trs :
            #self.log.info(f"tr  : {tr} / len tr  : {len(tr)} ")
            
            #self.log.info(f"tr  : {tr}")
        #     currency_name = li.find('a')
        #     currency_value = li.find('strong').findAll(text=True, recursive=False)[0]
        #     currency_value = re.search(r'(\d+,\d{3,4})', currency_value).group(1)
        #     currency_value = currency_value.replace(",", ".")
        #     country_names.append(country_name.text)
        #     currency_names.append(currency_name.text)
        #     currency_values.append(float(currency_value))

        return cities, departements, regions




    def create_currency_csv(self, country_names, currency_names, currency_values) :
        data = list(zip(country_names, currency_names, currency_values))
        df = pd.DataFrame(data, columns =['country_names', 'currency_names', 'currency_values'])
        df = cf.remove_accents(df=df)
        now = datetime.now()
        df['annee'] = now.year
        df['mois'] = now.month
        outdir = os.path.join(cf.OUTPUTS_DIR, 'currency', str(now.year))
        path = Path(outdir)
        path.mkdir(parents=True, exist_ok=True)
        saved_filename = f"currencies_{str(now.year)}.json"
        df.to_json(os.path.join(outdir, saved_filename), orient="records", lines=True)




    def execute(self, context):
        self.log.info(f"load currencies from  : {cf.CITY_URL}")
        cities, departements, regions = self.get_data_from(cf.CITY_URL)
        
        #self.log.info(f"create csv file from currencies.")
        #self.create_currency_csv(country_names, currency_names, currency_values)
        
        
    
    
    
