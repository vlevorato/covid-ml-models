CREATE OR REPLACE TABLE `{0}.viz_data`
as
(
  {1}

  select
  *,
  SUM(hosp_patients) OVER (ORDER BY historical_data.date) as total_patients_hospitalises,
  SUM(icu_patients) OVER (ORDER BY historical_data.date) as total_patients_reanimation,
  from `{0}.historical_data` as historical_data
  {2}
)