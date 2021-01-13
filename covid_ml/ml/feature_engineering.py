from datetime import datetime, timedelta
import pandas as pd
from dsbox.ml.feature_engineering.timeseries import RollingWindower, Shifter


def prepare_data(dataframe, location='France', date_col='date'):
    print('DF shape: {}'.format(dataframe.shape))
    if 'location' in dataframe.columns:
        dataframe = dataframe[dataframe['location'] == location]
    dataframe['date'] = pd.to_datetime(dataframe[date_col])
    dataframe = dataframe.resample('D', on=date_col).mean().reset_index(drop=False)
    dataframe = dataframe.interpolate(limit_area='inside')

    return dataframe


def merge_data(dataframe_list, merge_col='date'):
    return dataframe_list[0].merge(dataframe_list[1], on=merge_col, how='inner')


def create_features(dataframe, date_col='date', predict_period_days=15,
                    cols_to_shift=None, days_to_shift=None,
                    agg_ops=None, rolling_windows=None, shift_rolling_windows=None):
    dataframe = dataframe.sort_values(date_col)

    dataframe['total_cas_confirmes_1'] = dataframe['total_cas_confirmes'].shift(1)
    dataframe['new_cases_2'] = dataframe['total_cas_confirmes'] - dataframe['total_cas_confirmes_1']

    dataframe['prop_cases_vs_tests'] = dataframe['new_cases_2'] / dataframe['new_tests'].shift(1)
    dataframe['new_patients_gueris'] = dataframe['total_patients_gueris'] - \
                                       dataframe['total_patients_gueris'].shift(1)

    now_date = datetime.now().date()
    dates_to_predict = []
    for day_shift in range(1, predict_period_days):
        dates_to_predict.append(now_date + timedelta(days=day_shift))

    df_to_predict = pd.DataFrame({date_col: dates_to_predict})
    df_to_predict[date_col] = pd.to_datetime(df_to_predict[date_col])

    dataframe = pd.concat([dataframe, df_to_predict], sort=False).reset_index(drop=True)

    """
    Shifted and diff features
    """
    shifter = Shifter(shifts=days_to_shift)
    df_shifted = shifter.fit_transform(dataframe[cols_to_shift])
    for i in range(1, len(days_to_shift)):
        for col in cols_to_shift:
            df_shifted['diff_' + col + '_' + str(days_to_shift[i - 1]) + '_' + str(days_to_shift[i])] = \
                df_shifted[col + '_' + str(days_to_shift[i - 1])] - df_shifted[col + '_' + str(days_to_shift[i])]

    """
    Rolling windows shifted features
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

    dataframe = dataframe.join(df_roll_shift).join(df_shifted)

    return dataframe
