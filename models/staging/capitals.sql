{{ config(materialized='table') }}

SELECT 
    *
FROM {{ source('dbt_project', 'capitals')}}