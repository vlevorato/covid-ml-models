from dsbox.utils import execute_dag

from workflow import workflow_covidml_datascience

execute_dag(workflow_covidml_datascience.dag, verbose=True)
