from datetime import datetime
import pandas as pd


def dummy_function(dataframe):
    return dataframe


def export_predictions(dataframe, model_type=None, target=None):
    dataframe['model'] = model_type
    dataframe['target'] = target
    dataframe['date_export'] = datetime.now()
    dataframe['date_export'] = pd.to_datetime(dataframe['date_export'])

    return dataframe


def export_features_contribution(df_features, model_type=None, target=None):
    dataframe = pd.DataFrame({'features': df_features['feature'], 'importances': df_features['importance']})
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
