import os
from datetime import datetime

import pandas as pd
import numpy as np
from dsbox.ml.metrics import root_mean_squared_error
from dsbox.operators.data_unit import DataInputUnit
from dsbox.utils import write_object_file, load_object_file
from dsbox.ml.feature_selection.greedy import greedy_feature_selection
from eli5.sklearn import PermutationImportance

from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor, ExtraTreesRegressor
from sklearn.linear_model import BayesianRidge, ElasticNet
from sklearn.metrics import make_scorer
from sklearn.neighbors import KNeighborsRegressor
from sklearn.preprocessing import MinMaxScaler


def create_model(model_type='elastic_net'):
    if model_type == 'rf':
        return RandomForestRegressor(n_estimators=300,
                                     min_samples_leaf=2,
                                     max_depth=15,
                                     max_features=0.8,
                                     max_samples=0.8,
                                     n_jobs=1)

    if model_type == 'et':
        return ExtraTreesRegressor(n_estimators=300, min_samples_leaf=2, max_depth=7, max_features=0.8, n_jobs=1)

    if model_type == 'gbt':
        return GradientBoostingRegressor(n_estimators=500,
                                         learning_rate=0.01,
                                         random_state=42)

    if model_type == 'bridge':
        return BayesianRidge(normalize=True)

    if model_type == 'elastic_net':
        return ElasticNet(normalize=True, max_iter=100000, l1_ratio=0.95)

    if model_type == 'knn':
        return KNeighborsRegressor(n_neighbors=120, p=3, leaf_size=120, n_jobs=1)


def feature_contribution(model, features, model_type='elastic_net'):
    linear_models_type = ['elastic_net', 'bridge']
    ensemble_models_type = ['rf', 'gbt', 'et']

    if model_type in linear_models_type:
        df_features_contrib = pd.DataFrame({'feature': features, 'importance': model.coef_})
        return df_features_contrib

    if model_type in ensemble_models_type:
        df_features_contrib = pd.DataFrame(
            {'feature': features, 'importance': model.feature_importances_})
        return df_features_contrib

    df_features_contrib = pd.DataFrame(
        {'feature': features, 'importance': [np.nan] * len(features)})

    return df_features_contrib


def extract_feature_contribution(df_features, model_type_data_unit=None, model_path=None, target=None):
    df_model_type = model_type_data_unit.read_data()
    model_type = df_model_type['model_type'].values[0]
    print("Model type: {}".format(model_type))

    model = load_object_file(model_path + generate_model_filename(model_type, target))
    df_features_contrib = feature_contribution(model, df_features['features'], model_type=model_type)

    return df_features_contrib


def generate_model_filename(model_type, target):
    return model_type + '_' + target + '.model'


def check_features(features, col_name='features'):
    if isinstance(features, DataInputUnit):
        df_features = features.read_data()
        features = list(df_features[col_name].values)

    return features


def prepare_data(dataframe, features=None):
    mm_scaler = MinMaxScaler()
    dataframe[features] = mm_scaler.fit_transform(dataframe[features])
    return dataframe


def train(dataframe, date_col='date', model_type_data_unit=None, model_path=None, target=None, features=None,
          split_date=None):
    df_model_type = model_type_data_unit.read_data()
    model_type = df_model_type['model_type'].values[0]
    print("Model type: {}".format(model_type))

    features = check_features(features)
    dataframe = prepare_data(dataframe, features=features)

    X = dataframe.dropna(subset=features + [target])
    X = X[X[target] > 0].reset_index(drop=True)

    if split_date is not None:
        X = X[X[date_col] < split_date]

    print("Train dataset min: {}".format(X[date_col].min()))
    print("Train dataset max: {}".format(X[date_col].max()))

    model = create_model(model_type=model_type)
    model.fit(X[features], X[target])

    model_path += generate_model_filename(model_type, target)
    write_object_file(model_path, model)


def predict(dataframe, date_col='date', model_type_data_unit=None, model_path=None, target=None, features=None,
            y_pred_col='y_pred', split_date=None):
    df_model_type = model_type_data_unit.read_data()
    model_type = df_model_type['model_type'].values[0]
    print("Model type: {}".format(model_type))

    features = check_features(features)
    dataframe = prepare_data(dataframe, features=features)

    dataframe[features] = dataframe[features].fillna(method='ffill') \
        .fillna(method='bfill')

    X_to_predict = dataframe

    if split_date is None:
        X_to_predict = X_to_predict[pd.isnull(X_to_predict[target])]
        split_date = str(datetime.now().date())

    X_to_predict = X_to_predict[X_to_predict[date_col] >= split_date]

    print("Predict dataset min: {}".format(X_to_predict[date_col].min()))
    print("Predict dataset max: {}".format(X_to_predict[date_col].max()))

    model = load_object_file(model_path + generate_model_filename(model_type, target))
    y_pred = model.predict(X_to_predict[features])

    X_to_predict[y_pred_col] = y_pred
    X_to_predict[y_pred_col] = X_to_predict[y_pred_col].map(lambda x: 0 if x < 0 else x)

    return X_to_predict[[date_col, y_pred_col]]


def model_selection(dataframe, model_list, date_col='date', split_date=None, max_date=None, target=None, features=None,
                    score_func=root_mean_squared_error, cum_sum=False):
    dataframe = prepare_data(dataframe, features=features)
    X = dataframe.dropna(subset=features + [target])

    X_train = X[X[date_col] < split_date]
    X_test = X[X[date_col] >= split_date]
    if max_date is not None:
        X_test = X_test[X_test[date_col] < max_date]

    print("Train dataset min: {}".format(X_train[date_col].min()))
    print("Train dataset max: {}".format(X_train[date_col].max()))
    print("Test dataset min: {}".format(X_test[date_col].min()))
    print("Test dataset max: {}".format(X_test[date_col].max()))

    best_score = None
    best_model_type = None

    for model_type in model_list:
        print("Testing: {}".format(model_type))
        model = create_model(model_type)
        model.fit(X_train[features], X_train[target])
        y_test = model.predict(X_test[features])
        if cum_sum:
            score = score_func([X_test[target].cumsum().values[-1]], [np.cumsum(y_test)[-1]])
        else:
            score = score_func(X_test[target], y_test)

        print("Score: {}".format(score))

        if best_score is None or score < best_score:
            best_score = score
            best_model_type = model_type

    print("Best model: {}".format(best_model_type))
    df_best_model_type = pd.DataFrame({'target': [target], 'model_type': [best_model_type], 'score': [best_score]})
    return df_best_model_type


def permutation_importance_select_features(cols_to_test, model, df, target, score_func=root_mean_squared_error):
    scorer = make_scorer(score_func)
    perm = PermutationImportance(model, scoring=scorer, n_iter=3).fit(df[cols_to_test], df[target])
    perm_importance = pd.DataFrame({'feature': cols_to_test, 'importance': perm.feature_importances_}).sort_values(
        'importance', ascending=False)
    perm_importance = perm_importance[perm_importance['importance'] >= 0]
    return list(perm_importance['feature'].values)


def feature_selection(dataframe, date_col='date', split_date=None, max_date=None, model_type_data_unit=None,
                      method='greedy', score_func=root_mean_squared_error, target=None, features=None):
    df_model_type = model_type_data_unit.read_data()
    model_type = df_model_type['model_type'].values[0]
    print("Model type: {}".format(model_type))

    dataframe = prepare_data(dataframe, features=features)
    X = dataframe.dropna(subset=features + [target])

    X_train = X[X[date_col] < split_date]
    X_test = X[X[date_col] >= split_date]
    if max_date is not None:
        X_test = X_test[X_test[date_col] < max_date]

    print("Train dataset min: {}".format(X_train[date_col].min()))
    print("Train dataset max: {}".format(X_train[date_col].max()))
    print("Test dataset min: {}".format(X_test[date_col].min()))
    print("Test dataset max: {}".format(X_test[date_col].max()))

    cols_selected = features
    model = create_model(model_type)

    if method == 'greedy':
        cols_selected = greedy_feature_selection(X_train, X_test, X_train[target], X_test[target], model,
                                                 features, score_func)
    if method == 'permutation_importance' or method == 'filter_zero_coeff':
        model.fit(X_train[features], X_train[target])
        score = score_func(X_test[target], model.predict(X_test[features]))
        print("Original score: {}".format(score))
        if method == 'permutation_importance':
            cols_selected = permutation_importance_select_features(features, model, X_test, target)
        if method == 'filter_zero_coeff':
            df_features_contrib = feature_contribution(model, features, model_type=model_type)
            if len(df_features_contrib.dropna()) == 0:
                cols_selected = features
            else:
                cols_selected = list(df_features_contrib[df_features_contrib['importance'] > 0]['feature'])

        model.fit(X_train[cols_selected], X_train[target])
        new_score = score_func(X_test[target], model.predict(X_test[cols_selected]))
        print("New score: {}".format(new_score))
        if new_score >= score:
            print("No optim found :(")
            cols_selected = features

    if method == 'no_selection':
        cols_selected = features

    print('Features selected: {}'.format(len(cols_selected)))
    df_features = pd.DataFrame(cols_selected)
    df_features.columns = ['features']
    return df_features


def check_if_new_features_gives_better_model(data_unit, date_col='date', model_type_data_unit=None, target=None,
                                             current_features=None, candidates_features=None, split_date=None,
                                             score_func=root_mean_squared_error, task_id_update=None,
                                             task_id_skip=None):
    if not os.path.isfile(current_features.input_path):
        print("No features present.")
        return task_id_update

    df_model_type = model_type_data_unit.read_data()
    model_type = df_model_type['model_type'].values[0]
    print("Model type: {}".format(model_type))

    dataframe = data_unit.read_data()
    current_features = check_features(current_features)
    candidates_features = check_features(candidates_features)

    dataframe = prepare_data(dataframe, features=current_features + candidates_features)

    X = dataframe.dropna(subset=[target])
    X_train = X[X[date_col] < split_date]
    X_validation = X[X[date_col] >= split_date]

    print("Train dataset min: {}".format(X_train[date_col].min()))
    print("Train dataset max: {}".format(X_train[date_col].max()))
    print("Validation dataset min: {}".format(X_validation[date_col].min()))
    print("Validation dataset max: {}".format(X_validation[date_col].max()))

    current_model = create_model(model_type=model_type)
    current_model.fit(X_train[current_features].dropna(), X_train.dropna(subset=current_features)[target])
    y_pred_current = current_model.predict(X_validation[current_features].dropna())
    current_score = score_func(X_validation.dropna(subset=current_features)[target], y_pred_current)
    print("Current score: {}".format(current_score))

    new_model = create_model(model_type=model_type)
    new_model.fit(X_train[candidates_features].dropna(), X_train.dropna(subset=candidates_features)[target])
    y_pred_new = new_model.predict(X_validation[candidates_features].dropna())
    new_score = score_func(X_validation.dropna(subset=candidates_features)[target], y_pred_new)
    print("New score: {}".format(new_score))

    if new_score < current_score:
        print("Better model found!")
        return task_id_update
    else:
        print("No better model found...")
        return task_id_skip
