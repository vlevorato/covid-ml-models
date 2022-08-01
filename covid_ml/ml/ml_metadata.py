cols_to_shift = [# 'prop_cases_vs_tests',
                 'new_cases',
                 # 'new_tests',
                 'icu_patients',
                 'hosp_patients',
                 'reproduction_rate',
                 'new_deaths',
                 'people_vaccinated',
                 'people_fully_vaccinated']

ref_ops = {'mean': 'moyenne',
           'median': 'médiane',
           'std': 'écart-type',
           'min': 'minimum',
           'max': 'maximum'}

ref_cols = {'new_cases': 'Nouveaux cas',
            'hosp_patients': 'Nouveaux patients hospitalisés',
            'icu_patients': 'Nouveaux patients en réanimation',
            'new_deaths': 'Nouveaux décès',
            'prop_cases_vs_tests': 'Proportion de cas en fonction des tests',
            'new_tests': 'Nouveaux tests',
            'reproduction_rate': 'Taux de reproduction (Rt)',
            'people_vaccinated': 'Population avec au moins une dose de vaccin',
            'people_fully_vaccinated': 'Population avec un schéma vaccinal complet'
            }

ref_features = {}

agg_ops = ['mean', 'median', 'std', 'min', 'max']
rolling_windows = [3, 7, 14, 28]
shift_rolling_windows = [14, 21, 28]

cols_to_keep = []
for col in cols_to_shift:
    for i in range(1, len(shift_rolling_windows)):
        feature = 'diff_mean_7_' + col + '_' + str(shift_rolling_windows[i - 1]) + '_' + str(shift_rolling_windows[i])
        cols_to_keep.append(feature)
        ref_features[feature] = '{} - écart entre la moyenne sur 7j (-{}j) et (-{}j)'.format(ref_cols[col],
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

model_types = ['gbt', 'rf', 'et']  # , 'bridge', 'elastic_net' , 'knn']
targets = ['new_cases', 'hosp_patients', 'icu_patients', 'new_deaths']
targets_bq_mapping = {'new_cases': ['new_cases', 'new_cases_2'],
                      'hosp_patients': ['hosp_patients', 'nouveaux_patients_hospitalises'],
                      'icu_patients': ['icu_patients', 'nouveaux_patients_reanimation'],
                      'new_deaths': ['new_deaths']
                      }

target_feature_selection_method_dict = {'new_cases': 'permutation_importance',
                                        'hosp_patients': 'permutation_importance',
                                        'icu_patients': 'permutation_importance',
                                        'new_deaths': 'permutation_importance'}

ref_models = {'rf': 'Random Forest',
              'gbt': 'Gradient Tree Boosting',
              'elastic_net': 'Elastic Net',
              'bridge': 'Bayesian Ridge',
              'knn': 'K-Nearest Neighbors',
              'et': 'Extremely Randomized Trees'
              }
