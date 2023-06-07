{{ config(materialized='table') }}

SELECT 
    id
    , name as station_name
    , bikes_count
    , docks_count
    , install_date
    , removal_date
FROM {{ source('london_bicycles', 'cycle_stations')}}
WHERE install_date < '2017-01-01' and (removal_date < '2018-01-01' or removal_date is null)