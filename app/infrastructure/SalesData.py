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

    def __init__(self, path="") :
        self.path = path


    def clean_data(self) :
        name, extension = os.path.splitext(self.path)
        if extension == '.csv':
            df = pd.read_csv(self.path)
        elif extension == '.parquet':
            df = pd.read_parquet(self.path)
        else:
            raise FileExistsError('Extension must be parquet of csv.')

        df_sales = self._clean_data(df=df)
        return df_sales


    def _clean_data(self, df) : 
        df_with_renamed_columns = self.__change_columns_names__(df=df)
        df_with_formatted_date_columns = self.__change_date_to_date_type(df=df_with_renamed_columns)
        df_with_numeric_ca_columns = self.__change_turnover_to_numeric__(df=df_with_formatted_date_columns)
        df_with_cleaned_accents = self.__remove_accents__(df=df_with_numeric_ca_columns)
        return df_with_cleaned_accents

    

    def __change_columns_names__(self, df):
        col_names = [cf.DATE_COL, cf.EQUIPMENT_COL, cf.CITY_COL, cf.SALES_COL]
        new_df = df.copy()
        new_df.columns = col_names
        return new_df


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
    sd = SalesData(path=cf.FILE_DATA)
    print(sd.clean_data())









