# Most common streets where crashes happen in Nyc 


SELECT
    collision_id,
    crash_date,
    crash_time,
    borough,
    on_street_name,
    location,
    number_of_persons_injured,
    number_of_persons_killed,
    number_of_pedestrians_injured,
    number_of_pedestrians_killed,
    number_of_cyclist_injured,
    number_of_cyclist_killed,
    number_of_motorist_injured,
    number_of_motorist_killed,
    vehicle_type_code_1,
    contributing_factor_vehicle_1,
    COUNT(collision_id) AS total_crashes
FROM {{ ref("stg_2025_nyc_crashes") }}
GROUP BY
    collision_id,
    crash_date,
    crash_time,
    borough,
    on_street_name,
    location,
    number_of_persons_injured,
    number_of_persons_killed,
    number_of_pedestrians_injured,
    number_of_pedestrians_killed,
    number_of_cyclist_injured,
    number_of_cyclist_killed,
    number_of_motorist_injured,
    number_of_motorist_killed,
    vehicle_type_code_1,
    contributing_factor_vehicle_1
  
