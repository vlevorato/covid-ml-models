CREATE OR REPLACE TABLE `{0}.viz_data`
as
(
  {1}

  select
  *,
  SUM(nouveaux_patients_hospitalises) OVER (ORDER BY historical_data.date) as total_patients_hospitalises,
  SUM(nouveaux_patients_reanimation) OVER (ORDER BY historical_data.date) as total_patients_reanimation,
  from `{0}.historical_data` as historical_data
  {2}
)