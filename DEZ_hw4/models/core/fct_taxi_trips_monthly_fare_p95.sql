{{ config(materialized='table') }}


with valid_trips as (
    select *, 
    extract(year from pickup_datetime) as revenue_year,
    extract(month from pickup_datetime) as revenue_month
    from {{ ref('fct_trips') }}
    where fare_amount > 0
      and trip_distance > 0
      and payment_type_description in ('Cash', 'Credit card')
)
    select 
        service_type,
        revenue_year,
        revenue_month,
        fare_amount,
        ROUND(percentile_cont(fare_amount, 0.90) OVER (PARTITION BY service_type, revenue_year, revenue_month),2) AS fare_p90,
        ROUND(percentile_cont(fare_amount, 0.95) OVER (PARTITION BY service_type, revenue_year, revenue_month),2) AS fare_p95,
        ROUND(percentile_cont(fare_amount, 0.97) OVER (PARTITION BY service_type, revenue_year, revenue_month),2) AS fare_p97
    from valid_trips

