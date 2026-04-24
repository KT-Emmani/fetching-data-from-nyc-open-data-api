# Most common streets where crashes happen in Nyc 

SELECT
  on_street_name,
  COUNT(crash_date) AS total_crashes
FROM
  {{ ref ("stg_2025_nyc_crashes") }}
GROUP BY on_street_name
ORDER BY COUNT(crash_date) DESC
