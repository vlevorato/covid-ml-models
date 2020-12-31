from datetime import datetime

import pandas as pd
from dsbox.utils import write_object_file, load_object_file

from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor


def create_model(model_type='rf'):
    if model_type == 'rf':
        return RandomForestRegressor(n_estimators=200,
                                     min_samples_leaf=2,
                                     max_depth=15,
                                     max_features=0.8,
                                     n_jobs=1)

    if model_type == 'gbt':
        return GradientBoostingRegressor(n_estimators=400,
                                         learning_rate=0.01)


def generate_model_filename(model_type, target):
    return model_type + '_' + target + '.model'


def train(dataframe, date_col='date', model_type='rf', model_path=None, target=None, features=None, split_date=None):
    X = dataframe.dropna(subset=features + [target])

    if split_date is not None:
        X = X[X[date_col] < split_date]

    model = create_model(model_type=model_type)
    model.fit(X[features], X[target])

    model_path += generate_model_filename(model_type, target)
    write_object_file(model_path, model)


def predict(dataframe, date_col='date', model_type='rf', model_path=None, target=None, features=None,
            y_pred_col='y_pred', split_date=None):
    dataframe[features] = dataframe[features].fillna(method='ffill') \
        .fillna(method='bfill')

    X_to_predict = dataframe

    if split_date is None:
        X_to_predict = X_to_predict[pd.isnull(X_to_predict[target])]
        split_date = str(datetime.now().date())

    X_to_predict = X_to_predict[X_to_predict[date_col] >= split_date]

    model = load_object_file(model_path + generate_model_filename(model_type, target))
    y_pred = model.predict(X_to_predict[features])

    X_to_predict[y_pred_col] = y_pred

    return X_to_predict[[date_col, y_pred_col]]
