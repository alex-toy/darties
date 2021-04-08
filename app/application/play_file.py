from app.domain.BusinessData import BusinessData
import app.config.config as cf
import os

my_bd = BusinessData()
temp = my_bd.processed_data()

my_bd.save_data()

print('plot data')
my_bd.plot_data()

print('processed data')
print(temp)


