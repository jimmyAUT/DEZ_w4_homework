{{ config(materialized='table') }}


with valid_trips as (
    select *
    from {{ ref('fct_trips') }}
    where fare_amount > 0
      and trip_distance > 0
      and payment_type_description in ('Cash', 'Credit Card')
),

monthly_fare_percentiles as (
    select 
        service_type,
        extract(year from pickup_datetime) as revenue_year,
        extract(month from pickup_datetime) as revenue_month,
        ROUND(percentile_cont(0.90) within group (order by fare_amount),2) as fare_p90,
        ROUND(percentile_cont(0.95) within group (order by fare_amount),2) as fare_p95,
        ROUND(percentile_cont(0.97) within group (order by fare_amount),2) as fare_p97
    from valid_trips
    group by 1, 2, 3
)

select * from monthly_fare_percentiles;
