from app.infrastructure.SalesData import SalesData
import app.config.config as cf
import os




if __name__ == '__main__':

    df = SalesData.df_from_path(path=cf.FILE_DATA)
    sd = SalesData(df)
    year = '2020'
    sd.cleaned_file(year)




