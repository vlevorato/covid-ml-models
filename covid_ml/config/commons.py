from datetime import datetime, timedelta

from covid_ml.config.env_vars import config_variables

dag_start_date = datetime(2020, 12, 25)

# airflow common dag args
dag_args = {
    'start_date': dag_start_date,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'max_active_runs': 1
}

data_paths = {
    'raw_data_path': config_variables['COVIDML_DATA_PATH'] + 'raw_data/',
    'source_data_owid': "https://covid.ourworldindata.org/data/owid-covid-data.csv",
    'source_data_gov': "https://www.data.gouv.fr/en/datasets/r/d3a98a30-893f-47f7-96c5-2f4bcaaa0d71",
    'intermediate_data_path': config_variables['COVIDML_DATA_PATH'] + 'intermediate_data/',
    'features_path': config_variables['COVIDML_DATA_PATH'] + 'features/',
    'features_candidates_path': config_variables['COVIDML_DATA_PATH'] + 'features_candidates/'
}

