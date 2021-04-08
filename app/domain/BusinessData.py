# -*- coding: UTF-8 -*-
import numpy as np 
import pandas as pd
import os
import re
from datetime import datetime, timedelta, date
import calendar
from vacances_scolaires_france import SchoolHolidayDates
from jours_feries_france import JoursFeries
import holidays
import matplotlib.pyplot as plt

import app.config.config as cf

from app.infrastructure.SalesData import SalesData


class BusinessData :
    """
    add business data to cleaned data
    """

    def __init__(self) :
        self.sd = SalesData(path=cf.FILE_DATA).clean_data()


    def processed_data(self) :
        df = self.__filtered_data__()
        df = self.__add_weekday_weekend_columns__(df=df)
        df = self.__add_holidays_columns__(df=df)
        df = self.__add_bank_holidays_columns__(df=df)
        df = self.__add_distance_holiday_columns__(df=df)
        df = self.__add_ca_last_year_same_weekday__(df=df)
        self.processed_data = df
        return df


    def __filtered_data__(self) :
        new_df = self.sd.copy()
        is_city_to_retrieve = new_df[cf.CITY_COL].isin(cf.LIST_CITIES_TO_RETRIEVE)
        is_item_to_retrieve = (new_df[cf.EQUIPMENT_COL].isin(cf.LIST_PRODUCTS_TO_RETRIEVE))
        return new_df[ is_city_to_retrieve & is_item_to_retrieve ] 

    
    def __add_weekday_weekend_columns__(self, df) :
        new_df = df.copy()
        new_df[cf.WEEKDAY_COL] = new_df[cf.DATE_COL].apply(lambda row : calendar.day_name[row.weekday()] )
        new_df[cf.IS_WEEKEND_COL] = new_df[cf.WEEKDAY_COL].apply(lambda row : row in ['Sunday', 'Saturday'] )
        return new_df

    
    def __add_holidays_columns__(self, df) :
        d = SchoolHolidayDates()
        new_df = df.copy()
        new_df[cf.SCHOOL_HOLIDAYS_A_COL] = new_df[cf.DATE_COL].apply(lambda row : d.is_holiday_for_zone(date(row.year, row.month, row.day), 'A') )
        new_df[cf.SCHOOL_HOLIDAYS_B_COL] = new_df[cf.DATE_COL].apply(lambda row : d.is_holiday_for_zone(date(row.year, row.month, row.day), 'B') )
        new_df[cf.SCHOOL_HOLIDAYS_C_COL] = new_df[cf.DATE_COL].apply(lambda row : d.is_holiday_for_zone(date(row.year, row.month, row.day), 'C') )
        return new_df


    def __add_bank_holidays_columns__(self, df) :
        new_df = df.copy()
        years_list = new_df.loc[:, cf.DATE_COL].apply(lambda row : row.year).unique()
        hols = []
        for d in holidays.France(years=years_list).items() :
             hols.append(d[0])
        new_df[cf.IS_BANK_HOLIDAY_COL] = new_df[cf.DATE_COL].apply(lambda row : row in hols)
        return new_df


    def __add_distance_holiday_columns__(self, df) :
        new_df = df.copy()
        next_bank_holiday = new_df[cf.DATE_COL].apply(lambda row : JoursFeries.next_bank_holiday(row, zone="Métropole")[1])
        new_df[cf.DISTANCE_CLOSEST_BANK_HOLIDAY_COL] = (new_df[cf.DATE_COL] - next_bank_holiday).apply(lambda x: abs(x.days))
        return new_df


    def __add_ca_last_year_same_weekday__(self, df):
        new_df = df.copy()
        months = pd.to_datetime(new_df[cf.DATE_COL]).dt.month
        weekday = pd.to_datetime(new_df[cf.DATE_COL]).dt.weekday
        new_df[cf.SALES_LAST_YEAR_SAME_WEEKDAY] = new_df.groupby([months, weekday])[cf.SALES_COL].shift()
        return new_df


    def plot_data(self, title='Ventes', xlabel='Temps mensuel', ylabel='CA', dpi=100):
        year = np.array(pd.DatetimeIndex(self.processed_data[cf.DATE_COL]).year.astype(str), dtype=np.object)
        temp_month = (pd.DatetimeIndex(self.processed_data[cf.DATE_COL]).month.values).astype(str)
        month = np.array(np.char.zfill(temp_month, 2), dtype=np.object)  
        time = year + '-' + month
        
        self.processed_data['time'] = time
        CA_plot = self.processed_data[['time', cf.SALES_COL]].groupby(by=['time']).sum().sort_values(by='time').reset_index()

        plt.figure(figsize=(16,5), dpi=dpi)
        plt.xticks(rotation='vertical')
        plt.plot(CA_plot['time'], CA_plot[cf.SALES_COL], color='tab:blue')
        plt.gca().set(title=title, xlabel=xlabel, ylabel=ylabel)
        plt.show()

    def save_data(self) :
        self.processed_data.to_csv(cf.OUTPUTS_DIR + '/' + cf.SAVED_FILENAME)



if __name__ == '__main__':
    df = BusinessData()
    print(df.processed_data())








