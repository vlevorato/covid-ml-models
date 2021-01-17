cols_to_shift = ['prop_cases_vs_tests',
                 'new_cases_2',
                 'new_tests',
                 'nouveaux_patients_reanimation',
                 'nouveaux_patients_hospitalises',
                 'new_patients_gueris',
                 'reproduction_rate',
                 'new_deaths']

agg_ops = ['mean', 'median', 'std']
rolling_windows = [3, 7, 15, 30]
shift_rolling_windows = [14, 21, 28]

cols_to_keep = []
for col in cols_to_shift:
    for i in range(1, len(shift_rolling_windows)):
        cols_to_keep.append(
            'diff_mean_3_' + col + '_' + str(shift_rolling_windows[i - 1]) + '_' + str(shift_rolling_windows[i]))

    for agg_op in agg_ops:
        for rolling_window in rolling_windows:
            for shift_rolling_window in shift_rolling_windows:
                cols_to_keep.append('{}_{}_{}_{}'.format(agg_op, rolling_window, col, shift_rolling_window))

target_model_dict = {'new_cases_2': 'elastic_net',
                     'nouveaux_patients_hospitalises': 'elastic_net',
                     'nouveaux_patients_reanimation': 'elastic_net',
                     'new_deaths': 'gbt'}

target_feature_selection_method_dict = {'new_cases_2': 'no_selection',
                                        'nouveaux_patients_hospitalises': 'no_selection',
                                        'nouveaux_patients_reanimation': 'no_selection',
                                        'new_deaths': 'no_selection'}
