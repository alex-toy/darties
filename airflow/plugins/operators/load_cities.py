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



class LoadCitiesOperator(BaseOperator) :

    @apply_defaults
    def __init__(self,
                 *args, **kwargs):
        super(LoadCitiesOperator, self).__init__(*args, **kwargs)



    def get_data_from(self, url):
        r = requests.get(url)
        bs = BeautifulSoup(r.text, 'lxml')
        trs = bs.find_all('tr')
        filtered_trs = [ tr for tr in trs if len(tr.find_all('td'))>=14 ]

        cities = []
        departements = []
        regions = []

        for tr in filtered_trs :
            tds = tr.find_all('td')
            if len(tds) == 14 :
                city = tds[2].text
                city = re.search(r'[^[\n\r]+', city).group(0)
                departement = tds[3].find('a').text
                region = tds[5].text
            else :
                city = tds[1].text
                departement = tds[2].text
                region = tds[4].text

            city = city.replace("\n", "")
            departement = departement.replace("\n", "")
            region = region.replace("\n", "")

            cities.append(city)
            departements.append(departement)
            regions.append(region)

        return cities, departements, regions




    def create_cities_csv(self, cities, departements, regions) :
        data = list(zip(cities, departements, regions))
        df = pd.DataFrame(data, columns =['lib_ville', 'lib_departement', 'lib_reg_nouv'])
        df = cf.remove_accents(df=df)
        df['lib_pays'] = 'france'
        df['lib_continent'] = 'europe'
        now = datetime.now()
        outdir = os.path.join(cf.OUTPUTS_DIR, 'ville', str(now.year))
        path = Path(outdir)
        path.mkdir(parents=True, exist_ok=True)
        saved_filename = f"villes_{str(now.year)}.json"
        df.to_json(os.path.join(outdir, saved_filename), orient="records", lines=True)





    def execute(self, context):
        self.log.info(f"load currencies from  : {cf.CITY_URL_EUR}")
        cities, departements, regions = self.get_data_from(cf.CITY_URL_EUR)
        
        self.log.info(f"create csv file from cities.")
        self.create_cities_csv(cities, departements, regions)
        
        
    
    
    
