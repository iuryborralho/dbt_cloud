WITH stations AS (
    SELECT *
    FROM {{ ref('raw_bike_stations') }}
),

rides AS (
    SELECT *
    FROM {{ ref('cleaned_bike_rides') }}
),

start_stat_join AS (
    SELECT rides.*
    , stations.bikes_count as start_station_bikes_count
    , stations.docks_count as start_station_docks_count
    , stations.install_date as start_station_install_date
    FROM rides
    LEFT JOIN stations
    ON rides.start_station_id = stations.id
)

SELECT 
    total_minutes 
    , total_bike_hires 
    , average_duration 
    , month 
    , start_peak_travel
    , same_station_flag
    , start_station_id
    , start_station_name
    , start_station_bikes_count 
    , start_station_docks_count 
    , start_station_install_date 
    , end_station_id
    , end_station_name
    , stations.bikes_count as end_station_bikes_count
    , stations.docks_count as end_station_docks_count
    , stations.install_date as end_station_install_date
FROM start_stat_join
LEFT JOIN stations
ON start_stat_join.end_station_id = stations.id