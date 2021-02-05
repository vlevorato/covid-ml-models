CREATE OR REPLACE TABLE `{0}.viz_data_raw`
as
(
  SELECT
   *
  FROM
    `{0}.historical_data` AS historical_data
  FULL OUTER JOIN
    `{0}.predictions` as predictions
  USING (date)
)