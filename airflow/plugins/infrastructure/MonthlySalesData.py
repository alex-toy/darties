# -*- coding: UTF-8 -*-
import numpy as np 
import pandas as pd
from pandas.api.types import is_string_dtype
import os
import re
from datetime import datetime, timedelta, date
from pathlib import Path


import config.config as cf

class MonthlySalesData :
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



    def cleaned_file(self, year, month) :
        df = self.df_from_path()
        for sheet_name in self.sheet_names :
            df_sheet_name = df[sheet_name]
            df_sheet_name.columns = [x.lower() for x in df_sheet_name.columns]
            df_sheet_name = cf.remove_accents(df=df_sheet_name)
            outdir = os.path.join(cf.OUTPUTS_DIR, f"monthly_{sheet_name}", year)
            path = Path(outdir)
            path.mkdir(parents=True, exist_ok=True)
            saved_filename = f"{sheet_name}_{month}_{year}_sales.json"
            df_sheet_name['annee'] = int(year)
            df_sheet_name['lib_mois'] = month
            df_sheet_name.to_json(os.path.join(outdir, saved_filename), orient="records", lines=True)





if __name__ == '__main__':

    FILE_DATA = "/Users/alexei/docs150521/MIAGE/S4/D605/darties/airflow/data/monthly_sales/Janvier_2021.xlsx"
    
    msd = MonthlySalesData(path=FILE_DATA)
    msd.cleaned_file(year=2021)









