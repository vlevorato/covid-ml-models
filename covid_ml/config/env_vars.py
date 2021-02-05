from airflow.models import Variable
import os

"""
Using dictionary for config variables to be able to change the way to assign values.
(by default, using environment variables).
"""

config_variables = dict()
env_list = [
    'COVIDML_PROJECT_PATH',
    'COVIDML_DATA_PATH',
    'COVIDML_MODEL_PATH',
    'COVIDML_GCP_KEY_PATH',
    'COVIDML_BQ_DATASET',
    'COVIDML_BQ_CONN_ID'
]

for env_var in env_list:
    config_variables[env_var] = os.environ.get(env_var)

for env_var in env_list:
    if config_variables[env_var] is None:
        config_variables[env_var] = Variable.get(env_var)
