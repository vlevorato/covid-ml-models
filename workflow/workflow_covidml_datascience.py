from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from dsbox.operators.data_operator import DataOperator
from dsbox.operators.data_unit import DataInputFileUnit, DataOutputFileUnit, DataInputMultiFileUnit

from covid_ml.config.commons import dag_args, data_paths
from covid_ml.ml.feature_engineering import prepare_data, merge_data

dag = DAG(dag_id='covidml_data_science',
          default_args=dag_args,
          description='Data Science workflow for train-predict Covid insights',
          schedule_interval='10 0 * * *',  # every day at 00:10 am
          catchup=False)

task_prepare_data_done = DummyOperator(task_id='Prepare_data_done',
                                       dag=dag)

data_files_to_prepare = ['owid_data', 'datagov_data']

task_start = DummyOperator(task_id="Start", dag=dag)

task_group_prepare_data = TaskGroup("Prepare_data", dag=dag)

task_start.set_downstream(task_group_prepare_data)

for data_file in data_files_to_prepare:
    input_data_file_unit = DataInputFileUnit(data_paths['raw_data_path'] + data_file + '.csv')
    output_data_file_unit = DataOutputFileUnit(data_paths['intermediate_data_path'] + data_file + '.parquet',
                                               pandas_write_function_name='to_parquet')
    task_prepare_data = DataOperator(operation_function=prepare_data,
                                     input_unit=input_data_file_unit,
                                     output_unit=output_data_file_unit,
                                     task_id='Prepare_{}'.format(data_file),
                                     dag=dag)

    task_group_prepare_data.add(task_prepare_data)

task_group_prepare_data.set_downstream(task_prepare_data_done)

input_data_multi_files_unit = DataInputMultiFileUnit(
    [data_paths['intermediate_data_path'] + data_file + '.parquet' for data_file in data_files_to_prepare])
output_merge_unit = DataOutputFileUnit(data_paths['intermediate_data_path'] + 'X_merged.parquet',
                                       pandas_write_function_name='to_parquet')

task_merge_data = DataOperator(operation_function=merge_data,
                               input_unit=input_data_multi_files_unit,
                               output_unit=output_merge_unit,
                               task_id='Merge_data',
                               dag=dag)

task_prepare_data_done.set_downstream(task_merge_data)
