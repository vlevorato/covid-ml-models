from datetime import datetime

import pandas as pd
from dsbox.ml.metrics import root_mean_squared_error
from dsbox.utils import write_object_file, load_object_file
from dsbox.ml.feature_selection.greedy import greedy_feature_selection

from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.linear_model import BayesianRidge, ElasticNet


def create_model(model_type='elastic_net'):
    if model_type == 'rf':
        return RandomForestRegressor(n_estimators=200,
                                     min_samples_leaf=2,
                                     max_depth=15,
                                     max_features=0.8,
                                     n_jobs=1)

    if model_type == 'gbt':
        return GradientBoostingRegressor(n_estimators=400,
                                         learning_rate=0.01)

    if model_type == 'bridge':
        return BayesianRidge(normalize=True)

    if model_type == 'elastic_net':
        return ElasticNet(normalize=True, max_iter=100000, l1_ratio=0.9)


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
    X_to_predict[y_pred_col] = X_to_predict[y_pred_col].map(lambda x: 0 if x < 0 else x)

    return X_to_predict[[date_col, y_pred_col]]


def feature_selection(dataframe, date_col='date', split_date=None, model_type='elastic_net',
                      method='greedy', score_func=root_mean_squared_error, target=None, features=None):
    X = dataframe.dropna(subset=features + [target])

    X_train = X[X[date_col] < split_date]
    X_test = X[X[date_col] >= split_date]

    cols_selected = features
    model = create_model(model_type)

    if method == 'greedy':
        cols_selected = greedy_feature_selection(X_train, X_test, X_train[target], X_test[target], model,
                                                 features, score_func)

    return pd.DataFrame(cols_selected)
