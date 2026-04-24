# What factors contribute most to crashes in NYC?

WITH factors AS (
SELECT DISTINCT
  contributing_factors,
  COUNT(*) AS total_crashes,
  SUM(number_of_persons_killed) +
  SUM(number_of_pedestrians_killed) + 
  SUM(number_of_cyclist_killed) + 
  SUM(number_of_motorist_killed) AS death_toll,
  SUM(number_of_persons_injured) +
  SUM(number_of_pedestrians_injured) + 
  SUM(number_of_cyclist_injured) + 
  SUM(number_of_motorist_injured) AS injury_toll
FROM {{ ref('stg_2025_nyc_crashes')}},
UNNEST([
  contributing_factor_vehicle_1,
  contributing_factor_vehicle_2,
  contributing_factor_vehicle_3,
  contributing_factor_vehicle_4,
  contributing_factor_vehicle_5
]) AS contributing_factors
WHERE contributing_factors IS NOT NULL
GROUP BY contributing_factors
)
SELECT 
  f.contributing_factors,
  f.total_crashes,
  f.death_toll,
  f.injury_toll,
  SUM(f.death_toll) +
  SUM(f.injury_toll) AS total_casualties
FROM 
  factors f
GROUP BY f.contributing_factors, f.total_crashes, f.death_toll, f.injury_toll
ORDER BY 
  SUM(f.death_toll) +
  SUM(f.injury_toll) DESC  
