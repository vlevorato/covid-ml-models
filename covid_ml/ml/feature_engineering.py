from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from dsbox.ml.feature_engineering.timeseries import RollingWindower, Shifter


def preprocess_location(dataframe, location='France'):
    return dataframe[dataframe['location'] == location]


def preprocess_tests(dataframe):
    dataframe = dataframe[dataframe['cl_age90'] == 0]
    dataframe['date'] = dataframe['jour']
    del dataframe['jour']
    return dataframe


def preprocess_kpis(dataframe, date_col):
    dataframe = dataframe.replace('NA', np.nan)
    for col in dataframe.columns:
        if col != date_col:
            dataframe[col] = dataframe[col].astype('float')
    return dataframe


def prepare_data(dataframe, data_file=None, date_col='date'):
    print('DF shape: {}'.format(dataframe.shape))

    if data_file == 'owid_data':
        dataframe = preprocess_location(dataframe)
    if data_file == 'datagov_tests_data':
        dataframe = preprocess_tests(dataframe)
    if data_file == 'datagov_kpis_data':
        dataframe = preprocess_kpis(dataframe, date_col)

    dataframe['date'] = pd.to_datetime(dataframe[date_col])
    dataframe = dataframe.resample('D', on='date').mean().reset_index(drop=False)

    if data_file == 'owid_data':
        columns_to_process = list(dataframe.columns)
        columns_to_process.remove('date')

        for col in columns_to_process:
            dataframe[f'{col}_shift'] = dataframe[col].shift(1)
            dataframe[f'{col}_shift'] = dataframe[f'{col}_shift'].map(lambda x: 1 if x > 0 else 0)
            dataframe[col] = dataframe[col] * dataframe[f'{col}_shift']
            del dataframe[f'{col}_shift']

            dataframe[col] = dataframe[col].replace(0, np.nan)

    columns_to_process = list(dataframe.columns)
    columns_to_process.remove('date')
    dataframe[columns_to_process] = dataframe[columns_to_process].interpolate(limit_area='inside')
    dataframe[columns_to_process] = dataframe[columns_to_process].backfill()

    return dataframe


def merge_data(dataframe_list, merge_col='date'):
    dataframe = dataframe_list[0]
    for i in range(1, len(dataframe_list)):
        dataframe = dataframe.merge(dataframe_list[i], on=merge_col, how='left')
    return dataframe


def create_features(dataframe, date_col='date', predict_period_days=15, predict_period_week_round=False,
                    cols_to_shift=None, agg_ops=None, rolling_windows=None, shift_rolling_windows=None):
    dataframe = dataframe.sort_values(date_col)

    dataframe['prop_cases_vs_tests'] = dataframe['new_cases'] / dataframe['new_tests'].shift(1)

    now_date = datetime.now().date()
    dates_to_predict = []
    for day_shift in range(0, predict_period_days):
        dates_to_predict.append(now_date + timedelta(days=day_shift))

    if predict_period_week_round:
        while (now_date + timedelta(days=day_shift)).weekday() != 6:
            day_shift += 1
            dates_to_predict.append(now_date + timedelta(days=day_shift))

    df_to_predict = pd.DataFrame({date_col: dates_to_predict})
    df_to_predict[date_col] = pd.to_datetime(df_to_predict[date_col])

    dataframe = pd.concat([dataframe, df_to_predict], sort=False).reset_index(drop=True)

    """
    Misc features
    """
    dataframe['weekday'] = dataframe['date'].map(lambda d: pd.to_datetime(d).weekday())

    """
    Rolling windows shifted and diff features
    """
    df_roll = None
    for op in agg_ops:
        rolling_windower = RollingWindower(operation=op, windows=rolling_windows)
        if df_roll is None:
            df_roll = rolling_windower.fit_transform(dataframe[cols_to_shift])
        else:
            df_roll = df_roll.join(rolling_windower.fit_transform(dataframe[cols_to_shift]))

    shifter = Shifter(shifts=shift_rolling_windows)
    df_roll_shift = shifter.fit_transform(df_roll)

    for col in cols_to_shift:
        for i in range(1, len(shift_rolling_windows)):
            df_roll_shift['diff_mean_7_{}_{}_{}'.format(col, shift_rolling_windows[i - 1], shift_rolling_windows[i])] = \
                df_roll_shift['mean_7_{}_{}'.format(col, shift_rolling_windows[i - 1])] - df_roll_shift[
                    'mean_7_{}_{}'.format(col, shift_rolling_windows[i])]

    dataframe = dataframe.join(df_roll_shift)

    return dataframe
