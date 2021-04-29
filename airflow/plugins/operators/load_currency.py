from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

import os
import config.config as cf
from os import listdir
from os.path import isfile, join
import re
import requests


from webscraping.getcurrencies import getcurrencies



class LoadCurrencyOperator(BaseOperator) :

    @apply_defaults
    def __init__(self,
                 *args, **kwargs):
        super(LoadCurrencyOperator, self).__init__(*args, **kwargs)


    def get_data_from(self, url):
        r = requests.get(url)
        return r.text


    def execute(self, context):
        self.log.info(f"load currencies form  : {cf.CURRENCY_URL}")
        data = self.get_data_from(cf.CURRENCY_URL)
        self.log.info(f"data  : {data}")
        
        
        
    
    
    
