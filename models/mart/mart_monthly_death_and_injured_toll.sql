# Monthly Death and Injured Toll in Nyc 

WITH casualties AS (
SELECT
  FORMAT_DATE('%B', crash_date) AS month_name,
  SUM(number_of_persons_killed) +
  SUM(number_of_pedestrians_killed) + 
  SUM(number_of_cyclist_killed) + 
  SUM(number_of_motorist_killed) AS death_toll,
  SUM(number_of_persons_injured) +
  SUM(number_of_pedestrians_injured) + 
  SUM(number_of_cyclist_injured) + 
  SUM(number_of_motorist_injured) AS injury_toll
FROM
  {{ ref ("stg_2025_nyc_crashes") }}
GROUP BY month_name
)
SELECT
    c.month_name,
    c.death_toll,
    c.injury_toll,
    SUM(c.death_toll) +
    SUM(c.injury_toll) AS total_casualties
FROM 
    casualties c
GROUP BY c.month_name, c.death_toll, c.injury_toll
