import os

"""
Using dictionary for config variables to be able to change way to assign values.
(by default, using environment variables).
"""

config_variables = dict()
env_list = [
    'PROJECT_PATH',
    'DATA_PATH',
    'MODEL_PATH'
]

for env_var in env_list:
    config_variables[env_var] = os.environ.get(env_var)
