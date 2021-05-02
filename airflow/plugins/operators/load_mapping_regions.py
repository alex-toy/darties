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



#<tr class="odswidget-table__internal-table-row record-1"><td class="odswidget-table__cell"><div class="odswidget-table__cell-container">2</div></td><td class="odswidget-table__cell"><div class="odswidget-table__cell-container" dir="ltr"><span title="02">02</span></div></td><td class="odswidget-table__cell"><div class="odswidget-table__cell-container" dir="ltr"><span title="Martinique">Martinique</span></div></td><td class="odswidget-table__cell"><div class="odswidget-table__cell-container" dir="ltr"><span title="02">02</span></div></td><td class="odswidget-table__cell"><div class="odswidget-table__cell-container" dir="ltr"><span title="Martinique">Martinique</span></div></td><td class="odswidget-table__cell"><div class="odswidget-table__cell-container" dir="ltr"><span title="martinique">martinique</span></div></td><td class="odswidget-table__cell"><div class="odswidget-table__cell-container" dir="ltr"><span title="martinique">martinique</span></div></td><td class="odswidget-table__cell"><div class="odswidget-table__cell-container odswidget-table__cell-container__right-aligned"><span title="239&nbsp;720&nbsp;014">239&nbsp;720&nbsp;014</span></div></td><td class="odswidget-table__cell"><div class="odswidget-table__cell-container" dir="ltr"><span title="MARTINIQUE">MARTINIQUE</span></div></td></tr>



    def get_data_from(self, url):
        r = requests.get(url)
        bs = BeautifulSoup(r.text, 'lxml')
        trs = bs.find_all('tr', class_="odswidget-table__internal-table-row record-1")
        self.log.info(f"bs  : {trs}")

        # filtered_trs = [ tr for tr in trs if len(tr.find_all('td'))>=14 ]

        previous_names = []
        new_names = []

        # for tr in filtered_trs :
        #     tds = tr.find_all('td')
        #     if len(tds) == 14 :
        #         city = tds[2].text
        #         city = re.search(r'[^[\n\r]+', city).group(0)
        #         departement = tds[3].find('a').text
        #         region = tds[5].text
        #     else :
        #         city = tds[1].text
        #         departement = tds[2].text
        #         region = tds[4].text

        #     city = city.replace("\n", "")
        #     departement = departement.replace("\n", "")
        #     region = region.replace("\n", "")

        #     cities.append(city)
        #     departements.append(departement)
        #     regions.append(region)

        return previous_names, new_names




    def create_mapping_file(self, cities, departements, regions) :
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
        self.log.info(f"load mapping from  : {cf.MAPPING_PREV_NEW_REG}")
        previous_names, new_names = self.get_data_from(cf.MAPPING_PREV_NEW_REG)
        
        #self.log.info(f"create csv file from cities.")
        #self.create_mapping_file(cities, departements, regions)
        
        
    
    
    
