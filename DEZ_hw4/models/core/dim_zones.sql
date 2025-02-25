{{ config(materialized='table') }}

SELECT 
    locationid as location_id, 
    borough, 
    zone, 
    replace(service_zone, 'Boro', 'Green') as service_zone
FROM {{ref('taxi_zone_lookup')}}