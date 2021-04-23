# -*- coding: UTF-8 -*-
import numpy as np 
import pandas as pd
import os
import re
from datetime import datetime, timedelta, date
from pathlib import Path

import config.config as cf

class SalesData :
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
            df = df[sheet_name]
            outdir = os.path.join(cf.OUTPUTS_DIR, sheet_name, year)
            path = Path(outdir)
            path.mkdir(parents=True, exist_ok=True)
            saved_filename = f"{sheet_name}_{year}_sales.json"
            df = self.__add_year_col(df], year)
            df].to_json(os.path.join(outdir, saved_filename), orient="records")



    def __add_year_col(self, df, year) :
        new_df = df.copy()
        new_df['year'] = year
        return new_df






if __name__ == '__main__':
    
    sd = SalesData(path=cf.FILE_DATA)
    sd.cleaned_file(year='2021')









