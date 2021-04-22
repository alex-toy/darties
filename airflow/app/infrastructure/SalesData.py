# -*- coding: UTF-8 -*-
import numpy as np 
import pandas as pd
import os
import re
from datetime import datetime, timedelta, date
from pathlib import Path

import app.config.config as cf

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
            xl = pd.ExcelFile(self.path)
            self.sheet_names = xl.sheet_names
            return pd.read_excel(self.path, sheet_name=None)
        else:
            raise FileExistsError('Extension must be parquet or csv or xlsx.')


    def cleaned_file(self, year, saved_filename) :
        df = self._clean_data()
        for sheet_name in self.sheet_names :
            outdir = os.path.join(cf.OUTPUTS_DIR, sheet_name, year)
            path = Path(outdir)
            path.mkdir(parents=True, exist_ok=True)
            saved_filename = f"{sheet_name}_{year}_sales.json"
            df[sheet_name].to_json(os.path.join(outdir, saved_filename), orient="records")


    def _clean_data(self) :
        df = self.df_from_path()
        #df = self.__change_col_name(df=df)
        #df = self.__remove_accents__(df=df)
        return df


    def __change_col_name(self, df) :
        new_df = df.copy()
        new_df.rename(columns=cf.NEW_COL_NAMES, errors="raise", inplace=True)
        return new_df
							

    def __remove_accents__(self, df) :
        new_df = df.copy()
        for col in ['City', 'Brand', 'Region', 'Location'] :
            new_df[col] = new_df[col].str.replace('[éèê]', 'e', regex=True)
        return new_df



if __name__ == '__main__':
    
    sd = SalesData(path=cf.FILE_DATA)
    sd.cleaned_file(year='2021', saved_filename='sales.json')









