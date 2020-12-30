CREATE OR REPLACE TABLE `{0}.viz_data`
as
(
  {1}

  select
  historical_data.*,
  {2}
  from `{0}.historical_data` as historical_data
  {3}
)