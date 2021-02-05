from datetime import datetime, timedelta

from covid_ml.config.env_vars import config_variables

dag_start_date = datetime(2020, 12, 1)

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
    'source_data_gov_tests': "https://www.data.gouv.fr/fr/datasets/r/dd0de5d9-b5a5-4503-930a-7b08dc0adc7c",
    'source_data_gov_kpis': "https://www.data.gouv.fr/fr/datasets/r/381a9472-ce83-407d-9a64-1b8c23af83df",
    'intermediate_data_path': config_variables['COVIDML_DATA_PATH'] + 'intermediate_data/',
    'features_path': config_variables['COVIDML_DATA_PATH'] + 'features/',
    'features_candidates_path': config_variables['COVIDML_DATA_PATH'] + 'features_candidates/'
}

"""
Web pages sources:
https://ourworldindata.org/coronavirus-data
https://www.data.gouv.fr/fr/datasets/donnees-relatives-aux-resultats-des-tests-virologiques-covid-19/
https://www.data.gouv.fr/fr/datasets/donnees-relatives-a-lepidemie-de-covid-19-en-france-vue-densemble/
https://www.data.gouv.fr/fr/datasets/indicateurs-de-suivi-de-lepidemie-de-covid-19/
"""
