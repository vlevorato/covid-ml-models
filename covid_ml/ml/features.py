cols_to_shift = ['prop_cases_vs_tests',
                 'new_cases_2',
                 'new_tests',
                 'nouveaux_patients_reanimation',
                 'nouveaux_patients_hospitalises',
                 'new_patients_gueris',
                 'reproduction_rate',
                 'new_deaths']

agg_ops = ['mean', 'median', 'std', 'min', 'max']
rolling_windows = [3, 7, 15]
shift_rolling_windows = [15, 30]

cols_to_keep = []
for col in cols_to_shift:
    for agg_op in agg_ops:
        for rolling_window in rolling_windows:
            for shift_rolling_window in shift_rolling_windows:
                cols_to_keep.append('{}_{}_{}_{}'.format(agg_op, rolling_window, col, shift_rolling_window))
