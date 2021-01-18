from airflow import DAG
from airflow.operators.dummy import DummyOperator
from dsbox.operators.data_operator import DataOperator
from dsbox.operators.data_unit import DataInputFileUnit, DataOutputFileUnit

from covid_ml.config.commons import dag_args, data_paths
from covid_ml.utils.io import dummy_function

dag = DAG(dag_id='covidml_source_data_import',
          default_args=dag_args,
          description='Data source import',
          schedule_interval='0 0 * * *',  # every day at 00:00 am
          catchup=False)

task_start_import = DummyOperator(task_id='Start_source_data_import',
                                  dag=dag)

input_owid_data_unit = DataInputFileUnit(data_paths['source_data_owid'])
output_owid_data_unit = DataOutputFileUnit(data_paths['raw_data_path'] + 'owid_data.csv', index=False)
task_owid_import = DataOperator(operation_function=dummy_function,
                                input_unit=input_owid_data_unit,
                                output_unit=output_owid_data_unit,
                                task_id='Import_OWID_data',
                                dag=dag)

input_datagov_data_unit = DataInputFileUnit(data_paths['source_data_gov'])
output_datagov_data_unit = DataOutputFileUnit(data_paths['raw_data_path'] + 'datagov_data.csv', index=False)
task_datagov_import = DataOperator(operation_function=dummy_function,
                                   input_unit=input_datagov_data_unit,
                                   output_unit=output_datagov_data_unit,
                                   task_id='Import_DataGov_data',
                                   dag=dag)

input_datagov_tests_data_unit = DataInputFileUnit(data_paths['source_data_gov_test'], sep=';')
output_datagov_tests_data_unit = DataOutputFileUnit(data_paths['raw_data_path'] + 'datagov_tests_data.csv', index=False)
task_datagov_tests_import = DataOperator(operation_function=dummy_function,
                                         input_unit=input_datagov_tests_data_unit,
                                         output_unit=output_datagov_tests_data_unit,
                                         task_id='Import_DataGovTests_data',
                                         dag=dag)

task_start_import.set_downstream(task_owid_import)
task_start_import.set_downstream(task_datagov_import)
task_start_import.set_downstream(task_datagov_tests_import)
