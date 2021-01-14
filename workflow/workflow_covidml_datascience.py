from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from dsbox.operators.data_operator import DataOperator
from dsbox.operators.data_unit import DataInputFileUnit, DataOutputFileUnit, DataInputMultiFileUnit

from covid_ml.config.commons import dag_args, data_paths
from covid_ml.config.env_vars import config_variables
from covid_ml.ml.feature_engineering import prepare_data, merge_data, create_features
from covid_ml.ml.ml_metadata import cols_to_shift, agg_ops, rolling_windows, shift_rolling_windows, cols_to_keep, \
    targets, model_types, days_to_shift
from covid_ml.ml.model import train, predict, feature_selection, check_if_new_features_gives_better_model

dag = DAG(dag_id='covidml_data_science',
          default_args=dag_args,
          description='Data Science workflow for train-predict Covid insights',
          schedule_interval='5 0 * * *',  # every day at 00:05 am
          catchup=False)

"""
Data prep
"""

data_files_to_prepare = ['owid_data', 'datagov_data']

task_group_prepare_data = TaskGroup("Prepare_data", dag=dag)

for data_file in data_files_to_prepare:
    input_data_file_unit = DataInputFileUnit(data_paths['raw_data_path'] + data_file + '.csv')
    output_data_file_unit = DataOutputFileUnit(data_paths['intermediate_data_path'] + data_file + '.parquet',
                                               pandas_write_function_name='to_parquet')
    task_prepare_data = DataOperator(operation_function=prepare_data,
                                     input_unit=input_data_file_unit,
                                     output_unit=output_data_file_unit,
                                     task_id='Prepare_{}'.format(data_file),
                                     task_group=task_group_prepare_data,
                                     dag=dag)

input_data_multi_files_unit = DataInputMultiFileUnit(
    [data_paths['intermediate_data_path'] + data_file + '.parquet' for data_file in data_files_to_prepare],
    pandas_read_function_name='read_parquet')
output_merge_unit = DataOutputFileUnit(data_paths['intermediate_data_path'] + 'X_merged.parquet',
                                       pandas_write_function_name='to_parquet')

task_merge_data = DataOperator(operation_function=merge_data,
                               input_unit=input_data_multi_files_unit,
                               output_unit=output_merge_unit,
                               task_id='Merge_data',
                               dag=dag)

task_group_prepare_data.set_downstream(task_merge_data)

input_data_merged_unit = DataInputFileUnit(data_paths['intermediate_data_path'] + 'X_merged.parquet',
                                           pandas_read_function_name='read_parquet')
output_features_unit = DataOutputFileUnit(data_paths['intermediate_data_path'] + 'X_features.parquet',
                                          pandas_write_function_name='to_parquet')

"""
Feature Engineering
"""

task_fe = DataOperator(operation_function=create_features,
                       params={'cols_to_shift': cols_to_shift,
                               'days_to_shift': days_to_shift,
                               'agg_ops': agg_ops,
                               'rolling_windows': rolling_windows,
                               'shift_rolling_windows': shift_rolling_windows},
                       input_unit=input_data_merged_unit,
                       output_unit=output_features_unit,
                       task_id='Feature_engineering',
                       dag=dag)

task_merge_data.set_downstream(task_fe)

input_data_final_unit = DataInputFileUnit(data_paths['intermediate_data_path'] + 'X_features.parquet',
                                          pandas_read_function_name='read_parquet')

"""
Feature Selection
"""

split_date_feature_selection = datetime.now() - timedelta(days=15)

task_group_feature_selection = TaskGroup("Feature_selection", dag=dag)

for target in targets:
    for model_type in model_types:
        output_features_selection_unit = DataOutputFileUnit(data_paths['features_candidates_path']
                                                            + 'features_{}_{}.parquet'.format(model_type, target),
                                                            pandas_write_function_name='to_parquet')

        task_feature_selection = DataOperator(operation_function=feature_selection,
                                              params={'split_date': split_date_feature_selection,
                                                      'model_type': model_type,
                                                      'target': target,
                                                      'features': cols_to_keep},
                                              input_unit=input_data_final_unit,
                                              output_unit=output_features_selection_unit,
                                              task_group=task_group_feature_selection,
                                              task_id='Feature_selection_{}_{}'.format(model_type, target),
                                              dag=dag)

task_fe.set_downstream(task_group_feature_selection)

"""
Train model if none or better one is found
"""
split_date_for_train_predict = None

task_train_models = TaskGroup("Train", dag=dag)

task_dummy_start_train = DummyOperator(task_id='Start_train',
                                       task_group=task_train_models,
                                       dag=dag)

for target in targets:
    for model_type in model_types:
        input_features_selection_unit = DataInputFileUnit(data_paths['features_path']
                                                          + 'features_{}_{}.parquet'.format(model_type, target),
                                                          pandas_read_function_name='read_parquet')

        input_candidates_features_selection_unit = DataInputFileUnit(data_paths['features_candidates_path']
                                                                     + 'features_{}_{}.parquet'.format(model_type,
                                                                                                       target),
                                                                     pandas_read_function_name='read_parquet')

        task_check_if_retrain_needed = ShortCircuitOperator(python_callable=check_if_new_features_gives_better_model,
                                                            op_kwargs={'data_unit': input_data_final_unit,
                                                                       'model_type': model_type,
                                                                       'model_path': config_variables[
                                                                           'COVIDML_MODEL_PATH'],
                                                                       'target': target,
                                                                       'current_features': input_features_selection_unit,
                                                                       'candidates_features': input_candidates_features_selection_unit,
                                                                       'split_date': split_date_feature_selection
                                                                       },
                                                            task_id='Check_features_{}_{}'.format(model_type,
                                                                                                  target),
                                                            task_group=task_train_models,
                                                            dag=dag
                                                            )

        task_dummy_start_train.set_downstream(task_check_if_retrain_needed)

        task_copy_new_features = BashOperator(bash_command='cp {} {}'.format(data_paths['features_candidates_path']
                                                                             + 'features_{}_{}.parquet'.format(
            model_type, target),
                                                                             data_paths['features_path']),
                                              task_id='Update_features_{}_{}'.format(model_type, target),
                                              task_group=task_train_models,
                                              dag=dag)

        task_check_if_retrain_needed.set_downstream(task_copy_new_features)

        task_train = DataOperator(operation_function=train,
                                  params={'model_type': model_type,
                                          'model_path': config_variables['COVIDML_MODEL_PATH'],
                                          'target': target,
                                          'features': input_features_selection_unit,
                                          'split_date': split_date_for_train_predict},
                                  input_unit=input_data_final_unit,
                                  task_group=task_train_models,
                                  task_id='Train_model_{}_{}'.format(model_type, target),
                                  dag=dag)

        task_copy_new_features.set_downstream(task_train)

task_group_feature_selection.set_downstream(task_train_models)

"""
Predict
"""

task_predict_models = TaskGroup("Predict", dag=dag)

task_dummy_start_predict = DummyOperator(trigger_rule='all_done',
                                         task_id='Start_predictions',
                                         task_group=task_predict_models,
                                         dag=dag)

for target in targets:
    for model_type in model_types:
        input_features_selection_unit = DataInputFileUnit(data_paths['features_path']
                                                          + 'features_{}_{}.parquet'.format(model_type, target),
                                                          pandas_read_function_name='read_parquet')

        output_predictions_unit = DataOutputFileUnit(data_paths['intermediate_data_path'] +
                                                     'X_predict_{}_{}.parquet'.format(model_type, target),
                                                     pandas_write_function_name='to_parquet')
        task_predict = DataOperator(operation_function=predict,
                                    params={'model_type': model_type,
                                            'model_path': config_variables['COVIDML_MODEL_PATH'],
                                            'target': target,
                                            'features': input_features_selection_unit,
                                            'split_date': split_date_for_train_predict},
                                    input_unit=input_data_final_unit,
                                    output_unit=output_predictions_unit,
                                    task_group=task_predict_models,
                                    task_id='Predict_model_{}_{}'.format(model_type, target),
                                    dag=dag)

        task_dummy_start_predict.set_downstream(task_predict)

task_group_feature_selection.set_downstream(task_predict_models)
task_train_models.set_downstream(task_predict_models)

task_launch_export_predictions_dag = TriggerDagRunOperator(task_id='Trigger_export_predictions_dag',
                                                           trigger_dag_id='covidml_export_data_to_bq',
                                                           dag=dag)

task_predict_models.set_downstream(task_launch_export_predictions_dag)
