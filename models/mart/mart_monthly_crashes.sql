# Monthly crashes in Nyc 

SELECT
  FORMAT_DATE('%B', crash_date) AS month_name,
  COUNT(crash_date) AS total_crashes
FROM
  {{ ref ("stg_2025_nyc_crashes") }}
GROUP BY month_name
