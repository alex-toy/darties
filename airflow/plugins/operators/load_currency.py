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


from webscraping.getcurrencies import getcurrencies



class LoadCurrencyOperator(BaseOperator) :

    @apply_defaults
    def __init__(self,
                 *args, **kwargs):
        super(LoadCurrencyOperator, self).__init__(*args, **kwargs)



    def get_data_from(self, url):
        r = requests.get(url)
        soup = BeautifulSoup(r.text, 'lxml')
        li = soup.find_all('li', class_="currencylist-item")

        country_names = []
        currency_names = []
        currency_values = []

        for li in soup.find_all('li', class_="currencylist-item") :
            country_name = li.find('span')
            currency_name = li.find('a')
            currency_value = li.find('strong')
            country_names.append(country_name.text)
            currency_names.append(currency_name.text)
            currency_values.append(currency_value.text)

        return country_names, currency_names, currency_values




    def create_currency_csv(self, country_names, currency_names, currency_values) :
        data = list(zip(country_names, currency_names, currency_values))
        df = pd.DataFrame(data, columns =['country_names', 'currency_names', 'currency_values'])
        now = datetime.datetime.now()
        df['annee'] = now.year
        df['mois'] = now.month
        outdir = os.path.join(cf.OUTPUTS_DIR, 'currency', year)
        path = Path(outdir)
        path.mkdir(parents=True, exist_ok=True)
        saved_filename = f"currencies_{year}.json"
        df_sheet_name.to_json(os.path.join(outdir, saved_filename), orient="records", lines=True)




    def execute(self, context):
        self.log.info(f"load currencies from  : {cf.CURRENCY_URL}")
        country_names, currency_names, currency_values = self.get_data_from(cf.CURRENCY_URL)
        
        #self.log.info(f"country_name  : {country_names[0]} - currency_name  : {currency_names[0]} - currency_value  : {currency_values[0]}")
        self.log.info(f"create csv file from currencies.")
        self.create_currency_csv(country_names, currency_names, currency_values)
        
        
    
    
    
