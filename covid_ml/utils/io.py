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
