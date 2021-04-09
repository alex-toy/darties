import os
config_file = '/Users/alexei/docs/dwh_P6.cfg'
NAME_FILE = '2020_HISTO.xlsx'
FULL_PATH_DATA = os.path.os.getcwd()

REPO_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
DATA_DIR = os.path.join(REPO_DIR, 'data')
FILE_DATA = os.path.join(DATA_DIR, NAME_FILE)

OUTPUTS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../output'))
SAVED_FILENAME = 'processed_data.json'
OUTPUTS_FILE = os.path.join(OUTPUTS_DIR, SAVED_FILENAME)


