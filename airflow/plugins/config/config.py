import os
config_file = '/Users/alexei/docs150521/dwh_P6.cfg'

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

from pandas.api.types import is_string_dtype
def remove_accents(df) :
    new_df = df.copy()
    for col in new_df.columns :
        if is_string_dtype(new_df[col]) :
            new_df[col] = new_df[col].str.lower()
            new_df[col] = new_df[col].str.replace('[éèêë]', 'e', regex=True)
            new_df[col] = new_df[col].str.replace('[äàâ]', 'a', regex=True)
            new_df[col] = new_df[col].str.replace('[ûùü]', 'u', regex=True)
            new_df[col] = new_df[col].str.replace('[ïî]', 'i', regex=True)
            new_df[col] = new_df[col].str.replace('[ô]', 'o', regex=True)
            new_df[col] = new_df[col].str.replace('[ÿ]', 'y', regex=True)
            new_df[col] = new_df[col].str.replace('[ç]', 'c', regex=True)
            new_df[col] = new_df[col].str.replace('[œ]', 'oe', regex=True)

    return new_df


CURRENCY_URL = "https://www.capital.fr/bourse/devises/cours-devises"


CITY_URL_EUR = "https://fr.wikipedia.org/wiki/Liste_des_communes_de_France_les_plus_peuplées"


MAPPING_PREV_NEW_REG = "https://www.regions-et-departements.fr/regions-francaises"


MAPPING_DEP_PREV_REG = "https://www.axl.cefan.ulaval.ca/europe/france_departements.htm"