WITH source AS (

    SELECT
        DATE(SUBSTR(crash_date, 1, 10)) AS crash_date,
        SAFE.PARSE_TIME('%H:%M', crash_time) AS crash_time,
        on_street_name,
        off_street_name,
        SAFE_CAST(number_of_persons_injured AS INT64) AS number_of_persons_injured,
        SAFE_CAST(number_of_persons_killed AS INT64) AS number_of_persons_killed,
        SAFE_CAST(number_of_pedestrians_injured AS INT64) AS number_of_pedestrians_injured,
        SAFE_CAST(number_of_pedestrians_killed AS INT64) AS number_of_pedestrians_killed,
        SAFE_CAST(number_of_cyclist_injured AS INT64) AS number_of_cyclist_injured,
        SAFE_CAST(number_of_cyclist_killed AS INT64) AS number_of_cyclist_killed,
        SAFE_CAST(number_of_motorist_injured AS INT64) AS number_of_motorist_injured,
        SAFE_CAST(number_of_motorist_killed AS INT64) AS number_of_motorist_killed,
        contributing_factor_vehicle_1,
        contributing_factor_vehicle_2,
        CAST(collision_id AS INT64) AS collision_id,
        vehicle_type_code1 AS vehicle_type_code_1,
        vehicle_type_code2 AS vehicle_type_code_2,
        borough,
        zip_code,
        SAFE_CAST(latitude AS FLOAT64) AS latitude,
        SAFE_CAST(longitude AS FLOAT64) AS longitude,
        location,
        contributing_factor_vehicle_3,
        vehicle_type_code_3,
        cross_street_name,
        contributing_factor_vehicle_4,
        vehicle_type_code_4,
        contributing_factor_vehicle_5,
        vehicle_type_code_5
    FROM {{ source('dbt_nyc_motor_vehicle_collision', 'nyc_motor_crashes') }}
)
SELECT 
    *,
    current_timestamp() AS ingestion_timestamp 
FROM source


