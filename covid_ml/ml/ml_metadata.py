cols_to_shift = ['prop_cases_vs_tests',
                 'new_cases_2',
                 'new_tests',
                 'nouveaux_patients_reanimation',
                 'nouveaux_patients_hospitalises',
                 'new_patients_gueris',
                 'reproduction_rate',
                 'new_deaths']

ref_ops = {'mean': 'moyenne',
           'median': 'médiane',
           'std': 'écart-type'}

ref_cols = {'new_cases_2': 'Nouveaux cas',
            'nouveaux_patients_hospitalises': 'Nouveaux patients hospitalisés',
            'nouveaux_patients_reanimation': 'Nouveaux patients en réanimation',
            'new_deaths': 'Nouveaux décès',
            'prop_cases_vs_tests': 'Proportion de cas en fonction des tests',
            'new_tests': 'Nouveaux tests',
            'new_patients_gueris': 'Nouveaux patients guéris',
            'reproduction_rate': 'Taux de reproduction (Rt)'
            }

ref_features = {}

agg_ops = ['mean', 'median', 'std']
rolling_windows = [3, 7, 15, 30]
shift_rolling_windows = [14, 21, 28]

cols_to_keep = []
for col in cols_to_shift:
    for i in range(1, len(shift_rolling_windows)):
        feature = 'diff_mean_3_' + col + '_' + str(shift_rolling_windows[i - 1]) + '_' + str(shift_rolling_windows[i])
        cols_to_keep.append(feature)
        ref_features[feature] = '{} - écart entre la moyenne sur 3j (-{}j) et (-{}j)'.format(ref_cols[col],
                                                                                             shift_rolling_windows[
                                                                                                 i - 1],
                                                                                             shift_rolling_windows[i])

    for agg_op in agg_ops:
        for rolling_window in rolling_windows:
            for shift_rolling_window in shift_rolling_windows:
                feature = '{}_{}_{}_{}'.format(agg_op, rolling_window, col, shift_rolling_window)
                cols_to_keep.append(feature)
                ref_features[feature] = '{} - {} sur {}j (-{}j)'.format(ref_cols[col], ref_ops[agg_op], rolling_window,
                                                                        shift_rolling_window)

target_model_dict = {'new_cases_2': 'rf',
                     'nouveaux_patients_hospitalises': 'elastic_net',
                     'nouveaux_patients_reanimation': 'elastic_net',
                     'new_deaths': 'rf'}

target_feature_selection_method_dict = {'new_cases_2': 'filter_zero_coeff',
                                        'nouveaux_patients_hospitalises': 'filter_zero_coeff',
                                        'nouveaux_patients_reanimation': 'filter_zero_coeff',
                                        'new_deaths': 'filter_zero_coeff'}

ref_models = {'rf': 'Random Forest',
              'gbt': 'Gradient Tree Boosting (sklearn)',
              'elastic_net': 'Elastic Net',
              'bridge': 'Bayesian Ridge',
              'lgbm_200': 'Gradient Tree Boosting (LGBM)',
              'lgbm_300': 'Gradient Tree Boosting (LGBM)'
              }
