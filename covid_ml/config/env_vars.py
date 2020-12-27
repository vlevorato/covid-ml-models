from airflow.models import Variable
import os

"""
Using dictionary for config variables to be able to change way to assign values.
(by default, using environment variables).
"""

config_variables = dict()
env_list = [
    'COVIDML_PROJECT_PATH',
    'COVIDML_DATA_PATH',
    'COVIDML_MODEL_PATH'
]

for env_var in env_list:
    config_variables[env_var] = os.environ.get(env_var)

if len(config_variables) == 0:
    for env_var in env_list:
        config_variables[env_var] = Variable.get(env_var)
