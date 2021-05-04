# -*- coding: UTF-8 -*-
import numpy as np 
import pandas as pd
from pandas.api.types import is_string_dtype
import os
import re
from datetime import datetime, timedelta, date
from pathlib import Path


import config.config as cf

class StoreData :
    """
    cleans sales data from raw file
    """

    def __init__(self, path, sheet_name='darties') :
        self.path = path
        self.sheet_names = []

    
    def df_from_path(self):
        name, extension = os.path.splitext(self.path)
        if extension == '.csv':
            return pd.read_csv(path)
        elif extension == '.parquet':
            return pd.read_parquet(path)
        elif extension == '.xlsx':
            df = pd.read_excel(self.path, sheet_name=None, engine='openpyxl')
            self.sheet_names = df.keys()
            return df
        else:
            raise FileExistsError('Extension must be parquet or csv or xlsx.')



    def cleaned_file(self, year) :
        df = self.df_from_path()
        for sheet_name in self.sheet_names :
            df_sheet_name = df[sheet_name]
            df_sheet_name.columns = [x.lower() for x in df_sheet_name.columns]
            df_sheet_name = cf.remove_accents(df=df_sheet_name)
            outdir = os.path.join(cf.OUTPUTS_DIR, sheet_name, year)
            path = Path(outdir)
            path.mkdir(parents=True, exist_ok=True)
            saved_filename = f"{sheet_name}_{year}_stores.json"
            df_sheet_name['annee'] = year
            df_sheet_name.to_json(os.path.join(outdir, saved_filename), orient="records", lines=True)





if __name__ == '__main__':
    
    sd = SalesData(path=cf.FILE_DATA)
    sd.cleaned_file(year=cf.YEAR)









