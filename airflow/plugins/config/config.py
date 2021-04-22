import os
config_file = '/Users/alexei/docs/dwh_P6.cfg'
NAME_FILE = '2020_HISTO.xlsx'
YEAR = "2020"
FULL_PATH_DATA = os.path.os.getcwd()

REPO_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
DATA_DIR = os.path.join(REPO_DIR, 'data')
FILE_DATA = os.path.join(DATA_DIR, NAME_FILE)

OUTPUTS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../output'))
SAVED_FILENAME = 'sales.json'
OUTPUTS_FILE = os.path.join(OUTPUTS_DIR, SAVED_FILENAME)


NEW_COL_NAMES = {
    "Villes" : "City",
    "Enseignes" : "Brand",
    "Publicité": "Ads",
    "REGION" : "Region",
    "Emplacemen": "Location", 
    "Nb_Caisses": "Nb_cash_register",
    "Population": "Population",
    "Taux_Ouvri": "Blue_collar_rate",
    "Taux_Cadre": "White_collar_rate",
    "Taux_Inact": "Jobless_rate",
    "Moins_25an": "Lt_25_yo",
    "Les_25_35a": "25_35_yo",
    "Plus_35ans": "gt_35_yo",
}

