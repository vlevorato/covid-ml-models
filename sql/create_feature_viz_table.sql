CREATE OR REPLACE TABLE
  {0}.viz_feature_contribution AS (
  SELECT
    feature_contribution_last.*,
    (ABS(feature_contribution_last.importance) / feature_contribution_last_tot.total_importance) AS importance_proportion
  FROM
    `{0}.feature_contribution_last` AS feature_contribution_last
  INNER JOIN (
    SELECT
      SUM(ABS(importance)) AS total_importance,
      target
    FROM
      `{0}.feature_contribution_last`
    GROUP BY
      target ) AS feature_contribution_last_tot
  ON
    feature_contribution_last.target = feature_contribution_last_tot.target )