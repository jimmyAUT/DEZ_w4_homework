{{ config(materialized='table') }}

with trips_data as (
    select *, 
        extract(year from pickup_datetime) as year,
        extract(quarter from pickup_datetime) as quarter,
        concat(extract(year from pickup_datetime), '-Q', extract(quarter from pickup_datetime)) year_quarter
    from {{ ref('fct_trips') }}
    where extract(year from pickup_datetime) in (2019, 2020)
),

quarterly_revenue as (
    select 
        pickup_zone as revenue_zone,
        year,
        quarter,
        service_type,
        year_quarter,

        -- Revenue calculation
        sum(total_amount) as revenue_quarterly_total_amount,

        -- Additional calculations
        count(tripid) as total_quarterly_trips,
        avg(passenger_count) as avg_quarterly_passenger_count,
        avg(trip_distance) as avg_quarterly_trip_distance
    
    from trips_data
    group by 1, 2, 3, 4, 5
)

    SELECT 
        service_type,
        CASE 
            WHEN prev_year_revenue IS NOT NULL AND prev_year_revenue > 0
            THEN ((revenue_quarterly_total_amount - prev_year_revenue) / prev_year_revenue) * 100
            ELSE NULL 
        END AS yoy_growth_percentage
    FROM (
        SELECT 
            *,
            LAG(revenue_quarterly_total_amount, 4) OVER (PARTITION BY service_type, revenue_zone ORDER BY year, quarter) AS prev_year_revenue
        FROM 
            quarterly_revenue
        GROUP BY service_type
    ) AS subquery
    ORDER BY 
        year, quarter;