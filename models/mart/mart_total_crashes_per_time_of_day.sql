# Time of day that has the most crashes in Nyc 

SELECT
  CASE
    WHEN EXTRACT(HOUR FROM crash_time) = 0 THEN '12am - 1am'
    WHEN EXTRACT(HOUR FROM crash_time) = 1 THEN '1am - 2am'
    WHEN EXTRACT(HOUR FROM crash_time) = 2 THEN '2am - 3am'
    WHEN EXTRACT(HOUR FROM crash_time) = 3 THEN '3am - 4am'
    WHEN EXTRACT(HOUR FROM crash_time) = 4 THEN '4am - 5am'
    WHEN EXTRACT(HOUR FROM crash_time) = 5 THEN '5am - 6am'
    WHEN EXTRACT(HOUR FROM crash_time) = 6 THEN '6am - 7am'
    WHEN EXTRACT(HOUR FROM crash_time) = 7 THEN '7am - 8am'
    WHEN EXTRACT(HOUR FROM crash_time) = 8 THEN '8am - 9am'
    WHEN EXTRACT(HOUR FROM crash_time) = 9 THEN '9am - 10am'
    WHEN EXTRACT(HOUR FROM crash_time) = 10 THEN '10am - 11am'
    WHEN EXTRACT(HOUR FROM crash_time) = 11 THEN '11am - 12pm'
    WHEN EXTRACT(HOUR FROM crash_time) = 12 THEN '12pm - 1pm'
    WHEN EXTRACT(HOUR FROM crash_time) = 13 THEN '1pm - 2pm'
    WHEN EXTRACT(HOUR FROM crash_time) = 14 THEN '2pm - 3pm'
    WHEN EXTRACT(HOUR FROM crash_time) = 15 THEN '3pm - 4pm'
    WHEN EXTRACT(HOUR FROM crash_time) = 16 THEN '4pm - 5pm'
    WHEN EXTRACT(HOUR FROM crash_time) = 17 THEN '5pm - 6pm'
    WHEN EXTRACT(HOUR FROM crash_time) = 18 THEN '6pm - 7pm'
    WHEN EXTRACT(HOUR FROM crash_time) = 19 THEN '7pm - 8pm'
    WHEN EXTRACT(HOUR FROM crash_time) = 20 THEN '8pm - 9pm'
    WHEN EXTRACT(HOUR FROM crash_time) = 21 THEN '9pm - 10pm'
    WHEN EXTRACT(HOUR FROM crash_time) = 22 THEN '10pm - 11pm'
    ELSE '11pm - 12am'
  END AS time_bucket,
  COUNT(*) AS total_crashes
FROM {{ ref("stg_2025_nyc_crashes") }}
GROUP BY time_bucket
ORDER BY total_crashes DESC
