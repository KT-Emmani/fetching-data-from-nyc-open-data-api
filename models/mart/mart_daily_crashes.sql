# Which day of the week has the highest number of accidents in Nyc.

SELECT
  FORMAT_DATE('%A', crash_date) AS day_name,
  COUNT(crash_date) AS total_crashes
FROM
  {{ ref ("stg_2025_nyc_crashes") }}
GROUP BY day_name
ORDER BY COUNT(crash_date) DESC 
