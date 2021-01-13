cols_to_shift = ['prop_cases_vs_tests',
                 'new_cases_2',
                 'new_tests',
                 'nouveaux_patients_reanimation',
                 'nouveaux_patients_hospitalises',
                 'new_patients_gueris',
                 'reproduction_rate',
                 'new_deaths']

days_to_shift = [14, 21, 28]
agg_ops = ['mean', 'median', 'std', 'min', 'max']
rolling_windows = [3, 7, 15, 30]
shift_rolling_windows = [14, 21, 28]

cols_to_keep = []
for col in cols_to_shift:
    for d_shift in days_to_shift:
        cols_to_keep.append('{}_{}'.format(col, str(d_shift)))
    for i in range(1, len(days_to_shift)):
        cols_to_keep.append('diff_'+col+'_'+str(days_to_shift[i-1])+'_'+str(days_to_shift[i]))

    for agg_op in agg_ops:
        for rolling_window in rolling_windows:
            for shift_rolling_window in shift_rolling_windows:
                cols_to_keep.append('{}_{}_{}_{}'.format(agg_op, rolling_window, col, shift_rolling_window))

targets = ['new_cases_2', 'nouveaux_patients_hospitalises',
           'nouveaux_patients_reanimation', 'new_deaths']
model_types = ['bridge', 'elastic_net']
