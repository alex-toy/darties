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


    def clean_data(self) :
        df_sales = self._clean_data(df=self.df)
        return df_sales


    def _clean_data(self, df) :
        #df = self.__change_date_to_date_type(df=df)
        #df = self.__change_turnover_to_numeric__(df=df)
        #df = self.__remove_accents__(df=df)
        return df


    def __change_date_to_date_type(self, df) :
        new_df = df.copy()
        
        def row_into_date(row) :
            year = int(str(row)[4:8])
            month = int(str(row)[2:4].lstrip("0"))
            day = int(str(row)[0:2].lstrip("0"))
            return date(year, month, day)

        new_df[cf.DATE_COL] = df[cf.DATE_COL].apply(lambda row : row_into_date(row) )
        
        return new_df


    def __change_turnover_to_numeric__(self, df) :
        new_df = df.copy()
        new_df[cf.SALES_COL] = pd.to_numeric(new_df[cf.SALES_COL])
        return new_df


    def __remove_accents__(self, df) :
        new_df = df.copy()
        for col in [cf.EQUIPMENT_COL, cf.CITY_COL] :
            new_df[col] = new_df[col].str.replace('[éèê]', 'e', regex=True)
        return new_df



if __name__ == '__main__':
    df = SalesData.df_from_path(path=cf.FILE_DATA)
    sd = SalesData(df)
    print(sd.clean_data())









