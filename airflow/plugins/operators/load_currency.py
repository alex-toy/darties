from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

import os
import config.config as cf
from os import listdir
from os.path import isfile, join
import re
import requests
from bs4 import BeautifulSoup


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


    def execute(self, context):
        self.log.info(f"load currencies form  : {cf.CURRENCY_URL}")
        country_names, currency_names, currency_values = self.get_data_from(cf.CURRENCY_URL)
        self.log.info(f"country_name  : {country_names[0]} - currency_name  : {currency_names[0]} - currency_value  : {currency_values[0]}")
        
        
        
    
    
    
