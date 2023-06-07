{{ config(materialized='table') }}

SELECT 
    rental_id
    , duration as duration_seconds
    , duration / 60 as duration_minutes
    , bike_id
    , start_date
    , start_station_id
    , start_station_name
    , end_date
    , end_station_id
    , end_station_name
FROM {{ source('london_bicycles', 'cycle_hire')}}
WHERE EXTRACT(year from start_date) = 2017
