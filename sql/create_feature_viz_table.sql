CREATE OR REPLACE TABLE
  {0}.viz_feature_contribution AS (
  SELECT
    feature_contribution_last.*,
    ref_features.libelle AS feature_libelle,
    ref_models.libelle AS model_libelle,
    ref_cols.libelle AS target_libelle,
    (ABS(feature_contribution_last.importance) / sum_importance) AS importance_proportion
  FROM
    `{0}.feature_contribution_last` AS feature_contribution_last
  LEFT JOIN (
    SELECT
      SUM(ABS(importance)) AS sum_importance,
      target
    FROM
      `{0}.feature_contribution_last`
    GROUP BY
      target ) AS feature_contribution_last_tot
  ON
    feature_contribution_last.target = feature_contribution_last_tot.target
  LEFT JOIN
    `{0}.ref_features` AS ref_features
  ON
    feature_contribution_last.feature = ref_features.variable
  LEFT JOIN
    `{0}.ref_models` AS ref_models
  ON
    feature_contribution_last.model = ref_models.variable
  LEFT JOIN
    `{0}.ref_cols` AS ref_cols
  ON
    feature_contribution_last.target = ref_cols.variable )