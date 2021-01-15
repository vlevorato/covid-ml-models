def generate_data_viz_query(template_query, joining_field='date',
                            bq_dataset=None, targets=None):
    pre_query = 'WITH'
    for target in targets:
        pre_query += ' predictions_data_{0} AS ' \
                     '( SELECT ' \
                     '{1}, ' \
                     'CAST(AVG(y_pred) AS INT64) as {0}_pred ' \
                     'FROM `{2}.predictions_last` as predictions_last ' \
                     "WHERE target = '{0}' " \
                     "GROUP BY predictions_last.{1} ),".format(target, joining_field, bq_dataset)

    pre_query = pre_query[:-1]

    select_query = ''
    for target in targets:
        select_query += 'predictions_data_{0}.* EXCEPT({1}), \n'.format(target, joining_field)

    select_query = select_query[:-1]

    join_query = ''
    for target in targets:
        join_query += 'FULL OUTER JOIN predictions_data_{}\n'.format(target)
        join_query += 'ON historical_data.{0} = predictions_data_{1}.{0}\n'.format(joining_field, target)

    query = template_query.format(bq_dataset, pre_query, select_query, join_query)
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

    select_query = ''
    for target in targets:
        select_query += 'predictions_data_{0}.* EXCEPT({1}), \n'.format(target, joining_field)

    select_query = select_query[:-1]

    join_query = ''
    for target in targets:
        join_query += 'FULL OUTER JOIN predictions_data_{}\n'.format(target)
        join_query += 'ON historical_data.{0} = predictions_data_{1}.{0}\n'.format(joining_field, target)

    query = template_query.format(bq_dataset, pre_query, select_query, join_query)
    return query
