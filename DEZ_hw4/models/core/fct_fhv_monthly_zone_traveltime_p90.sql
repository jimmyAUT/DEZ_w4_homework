{{ config(materialized='table') }}


with fhv_trips as (
    select *, timestamp_diff(dropoff_datetime, pickup_datetime, SECOND) as trip_duration
    from {{ ref('fct_fhv_trips') }}
)
    select
        tripid,
        pickup_zone,
        dropoff_zone, 
        pickup_year,
        pickup_month,
        pickup_locationid,
        dropoff_locationid,
        percentile_cont(trip_duration, 0.90) OVER (PARTITION BY pickup_year, pickup_month, pickup_locationid, dropoff_locationid) AS trip_duration_p90
    from fhv_trips
