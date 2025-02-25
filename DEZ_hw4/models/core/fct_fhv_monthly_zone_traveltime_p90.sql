{{ config(materialized='table') }}


with fhv_trips as (
    select *, timestamp_diff(dropoff_datetime, pickup_datetime, SECOND) as trip_duration,
    from {{ ref('fct_fhv_trips') }}
),

final as (
    select
        tripid,
        pickup_zone,
        dropoff_zone, 
        pickup_year,
        pickup_month,
        pickup_locationid,
        dropoff_locationid,
        percentile_cont(0.90) within group (order by trip_duration) as trip_duration_p90
    from fhv_trips
    group by 1, 2, 3, 4
)

select * from final;