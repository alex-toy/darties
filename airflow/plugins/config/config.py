import os
config_file = '/Users/alexei/docs/dwh_P6.cfg'

FULL_PATH_DATA = os.path.os.getcwd()

REPO_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
DATA_DIR = os.path.join(REPO_DIR, 'data')

OUTPUTS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../output'))



MONTHS = ['janvier', 'fevrier', 'mars', 'avril', 'mai', 'juin', 'juillet', 'aout', 'septembre', 'octobre', 'novembre', 'decembre']

NUM_TO_NAME_MONTH = {
    1 : 'janvier',
    2 : 'fevrier',
    3 : 'mars',
    4 : 'avril',
    5 : 'mai',
    6 : 'juin',
    7 : 'juillet',
    8 : 'aout',
    9 : 'septembre',
    10 : 'octobre',
    11 : 'novembre',
    12 : 'decembre',
}





CURRENCY_URL = "https://www.capital.fr/bourse/devises/cours-devises"