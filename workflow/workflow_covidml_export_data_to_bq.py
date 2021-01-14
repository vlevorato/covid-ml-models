from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.task_group import TaskGroup
from dsbox.operators.data_operator import DataOperator
from dsbox.operators.data_unit import DataInputFileUnit

from covid_ml.config.commons import dag_args, data_paths
from covid_ml.config.env_vars import config_variables
from covid_ml.ml.ml_metadata import targets, model_types
from covid_ml.utils.bq_generation import generate_data_viz_query
from covid_ml.utils.bq_units import DataOutputBigQueryUnit
from covid_ml.utils.io import dummy_function, export_predictions, get_bq_query

dag = DAG(dag_id='covidml_export_data_to_bq',
          default_args=dag_args,
          description='Workflow exporting prediction data to BQ',
          schedule_interval=None,
          catchup=False)

path_json_key = config_variables['COVIDML_GCP_KEY_PATH']
bq_dataset = config_variables['COVIDML_BQ_DATASET']

input_histo_data_unit = DataInputFileUnit(data_paths['intermediate_data_path'] +
                                          'X_features.parquet',
                                          pandas_read_function_name='read_parquet')
output_histo_bq_unit = DataOutputBigQueryUnit(table_id='{}.historical_data'.format(bq_dataset),
                                              path_json_key=path_json_key)

task_export_historical_data = DataOperator(operation_function=dummy_function,
                                           input_unit=input_histo_data_unit,
                                           output_unit=output_histo_bq_unit,
                                           task_id='Export_historical_data',
                                           dag=dag)

task_group_export_predictions = TaskGroup("Export_predictions", dag=dag)

for target in targets:
    for model_type in model_types:
        input_predictions_unit = DataInputFileUnit(data_paths['intermediate_data_path'] +
                                                   'X_predict_{}_{}.parquet'.format(model_type, target),
                                                   pandas_read_function_name='read_parquet')
        output_pred_bq_unit = DataOutputBigQueryUnit(table_id='{}.predictions'.format(bq_dataset),
                                                     path_json_key=path_json_key,
                                                     drop_table=False)
        task_export_predictions_data = DataOperator(operation_function=export_predictions,
                                                    params={'model_type': model_type,
                                                            'target': target},
                                                    input_unit=input_predictions_unit,
                                                    output_unit=output_pred_bq_unit,
                                                    task_group=task_group_export_predictions,
                                                    task_id='Export_predictions_{}_{}'.format(model_type, target),
                                                    dag=dag)

task_export_historical_data.set_downstream(task_group_export_predictions)

data_viz_table_query = generate_data_viz_query(get_bq_query('create_data_viz_table_template',
                                                            config_variables['COVIDML_PROJECT_PATH']),
                                               bq_dataset=config_variables['COVIDML_BQ_DATASET'],
                                               targets=targets)

task_generate_data_viz_table = BigQueryInsertJobOperator(gcp_conn_id=config_variables['COVIDML_BQ_CONN_ID'],
                                                         configuration={"query": {"query": data_viz_table_query,
                                                                                  "useLegacySql": "False", }},
                                                         task_id='Generate_data_viz_table',
                                                         dag=dag)

task_group_export_predictions.set_downstream(task_generate_data_viz_table)
