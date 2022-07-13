import pandas as pd


def generate_data_viz_query(template_query, joining_field='date',
                            bq_dataset=None, targets=None):
    pre_query = 'WITH'
    for target, fields in targets.items():
        fields = str(fields)
        fields = fields.replace('[', '')
        fields = fields.replace(']', '')
        from_table = '`{}.predictions_last` as predictions_last'.format(bq_dataset)
        if target == 'hosp_patients':
            from_table = '(SELECT date, model, target, date_export, ' \
                         "IF(target='nouveaux_patients_hospitalises', y_pred * 10, y_pred) as y_pred" \
                         ' FROM `{0}.predictions_last`) as predictions_last'.format(bq_dataset)
        pre_query += ' predictions_data_{0} AS ' \
                     '( SELECT ' \
                     '{1}, ' \
                     'CAST(AVG(y_pred) AS INT64) as {0}_pred ' \
                     'FROM {3} ' \
                     "WHERE target in ({2}) " \
                     "GROUP BY predictions_last.{1} ),".format(target, joining_field, fields, from_table)

    pre_query = pre_query[:-1]

    join_query = ''
    for target in targets:
        join_query += 'FULL OUTER JOIN predictions_data_{}\n'.format(target)
        join_query += 'USING ({})\n'.format(joining_field)

    query = template_query.format(bq_dataset, pre_query, join_query)
    return query


def generate_data_viz_raw_query(template_query, joining_field='date',
                                bq_dataset=None, targets=None):
    pre_query = 'WITH'
    for target in targets:
        pre_query += ' predictions_data_{0} AS ' \
                     '( SELECT ' \
                     '{1}, ' \
                     'date_export as date_export_{0}, ' \
                     'CAST(y_pred AS INT64) as {0}_pred, ' \
                     'model as model_{0} ' \
                     'FROM `{2}.predictions` as predictions ' \
                     "WHERE target = '{0}'),".format(target, joining_field, bq_dataset)

    pre_query = pre_query[:-1]

    join_query = ''
    for target in targets:
        join_query += 'FULL OUTER JOIN predictions_data_{}\n'.format(target)
        join_query += 'USING ({})\n'.format(joining_field)

    query = template_query.format(bq_dataset, pre_query, join_query)
    return query


def generate_referential(ref_dict):
    variable_names = ref_dict.keys()
    col_names = ref_dict.values()
    df_ref = pd.DataFrame({'variable': variable_names, 'libelle': col_names})
    return df_ref
