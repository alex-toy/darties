import os
NAME_FILE = 'second_dataset.parquet'
FULL_PATH_DATA = os.path.os.getcwd()

REPO_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
DATA_DIR = os.path.join(REPO_DIR, 'data')
FILE_DATA = os.path.join(DATA_DIR, NAME_FILE)

OUTPUTS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../output'))
SAVED_FILENAME = 'processed_data.csv'


EQUIPMENT_COL = 'equipment'
CITY_COL = 'city'
DATE_COL = 'date'
SALES_COL = 'ca'


LIST_PRODUCTS_TO_RETRIEVE = ['ordinateur']
LIST_CITIES_TO_RETRIEVE = ['Paris']


BANK_HOLIDAYS_DATE_COL = 'date'
BANK_HOLIDAYS_NAME_COL = 'name'

SCHOOL_HOLIDAYS_A_RAW_COL = 'vacances_zone_a'
SCHOOL_HOLIDAYS_B_RAW_COL = 'vacances_zone_b'
SCHOOL_HOLIDAYS_C_RAW_COL = 'vacances_zone_c'
SCHOOL_HOLIDAYS_NAME_RAW_COL = 'nom_vacances'
SCHOOL_HOLIDAYS_A_COL = 'bool_school_holiday_A'
SCHOOL_HOLIDAYS_B_COL = 'bool_school_holiday_B'
SCHOOL_HOLIDAYS_C_COL = 'bool_school_holiday_C'
SCHOOL_HOLIDAYS_NAME_COL = 'school_holiday_name'


SALES_LAST_YEAR_COL = 'ca_last_year'
SALES_LAST_YEAR_SAME_WEEKDAY = 'ca_last_year_same_weekday'
WEEKDAY_COL = 'weekday'
IS_WEEKEND_COL = 'is_weekend'
IS_BANK_HOLIDAY_COL = 'is_bankholiday'
DISTANCE_CLOSEST_BANK_HOLIDAY_COL = 'distance_between_closest_bank_holiday'
IS_SCHOOL_HOLIDAY_COL = 'is_schoolholiday'

