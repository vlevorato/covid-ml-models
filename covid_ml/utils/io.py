from datetime import datetime
import pandas as pd


def dummy_function(dataframe):
    return dataframe


def export_data(dataframe, model_type_data_unit=None, target=None):
    df_model_type = model_type_data_unit.read_data()
    model_type = df_model_type['model_type'].values[0]
    print("Model type: {}".format(model_type))

    dataframe['model'] = model_type
    dataframe['target'] = target
    dataframe['date_export'] = datetime.now()
    dataframe['date_export'] = pd.to_datetime(dataframe['date_export'])

    return dataframe


def get_bq_query(query_name, file_path):
    bq_file = '{}sql/{}.sql'.format(file_path, query_name)
    f = open(bq_file, "r")
    query = f.read()
    f.close()
    return query
