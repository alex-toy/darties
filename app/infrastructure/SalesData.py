# -*- coding: UTF-8 -*-
import numpy as np 
import pandas as pd
import os
import re
from datetime import datetime, timedelta, date


#import app.config.config as cf
import app.config.config as cf


class SalesData :
    """
    cleans sales data from raw file
    """

    def __init__(self, df) :
        self.df = df

    
    @classmethod
    def df_from_path(cls, path=""):
        name, extension = os.path.splitext(path)
        if extension == '.csv':
            return pd.read_csv(path)
        elif extension == '.parquet':
            return pd.read_parquet(path)
        elif extension == '.xlsx':
            return pd.read_excel(path)
        else:
            raise FileExistsError('Extension must be parquet or csv or xlsx.')


    def cleaned_file(self) :
        df = self._clean_data(df=self.df)
        df.to_json (cf.OUTPUTS_FILE, orient="records")


    def _clean_data(self, df) :
        df = self.__change_col_name(df=df)
        df = self.__remove_accents__(df=df)
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
    df = SalesData.df_from_path(path=cf.FILE_DATA)
    sd = SalesData(df)
    sd.cleaned_file()









