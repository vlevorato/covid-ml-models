import pandas as pd


def prepare_data(dataframe, location='France', date_col='date'):
    print('DF shape: {}'.format(dataframe.shape))
    if location in dataframe.columns:
        dataframe = dataframe[dataframe['location'] == location]
    dataframe['date'] = pd.to_datetime(dataframe[date_col])
    dataframe = dataframe.resample('D', on=date_col).mean().reset_index(drop=False)
    dataframe = dataframe.interpolate()

    return dataframe


def merge_data(dataframe_list, merge_col='date'):
    return dataframe_list[0].merge(dataframe_list[1], on=merge_col, how='inner')
