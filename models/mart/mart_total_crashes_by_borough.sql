# Total crashes by Borough in Nyc 

SELECT
  Borough,
  COUNT(crash_date) AS total_crashes
FROM
  {{ ref ("stg_2025_nyc_crashes") }}
GROUP BY Borough
ORDER BY COUNT(crash_date) DESC 
