{{ config(materialized='table') }}

with trips_data as (
    select * from {{ ref('fct_trips') }}
),

quarterly_revenue as (
    select 
        pickup_zone as revenue_zone,
        extract(year from pickup_datetime) as revenue_year,
        extract(quarter from pickup_datetime) as revenue_quarter,
        service_type,
        concat(extract(year from pickup_datetime), '-Q', extract(quarter from pickup_datetime)) year_quarter,

        -- Revenue calculation
        sum(fare_amount) as revenue_quarterly_fare,
        sum(extra) as revenue_quarterly_extra,
        sum(mta_tax) as revenue_quarterly_mta_tax,
        sum(tip_amount) as revenue_quarterly_tip_amount,
        sum(tolls_amount) as revenue_quarterly_tolls_amount,
        sum(ehail_fee) as revenue_quarterly_ehail_fee,
        sum(improvement_surcharge) as revenue_quarterly_improvement_surcharge,
        sum(total_amount) as revenue_quarterly_total_amount,

        -- Additional calculations
        count(tripid) as total_quarterly_trips,
        avg(passenger_count) as avg_quarterly_passenger_count,
        avg(trip_distance) as avg_quarterly_trip_distance
    
    from trips_data
    group by 1, 2, 3, 4, 5
)


    select 
        q1.*, 
        q2.revenue_quarterly_total_amount as prev_year_revenue,
        
        -- Year-on-Year Growth Calculation
        case when q2.revenue_quarterly_total_amount is not null and q2.revenue_quarterly_total_amount > 0 
            then ((q1.revenue_quarterly_total_amount - q2.revenue_quarterly_total_amount) / q2.revenue_quarterly_total_amount) * 100
            else null 
        end as yoy_growth_percentage
    
    from quarterly_revenue q1
    left join quarterly_revenue q2
        on q1.revenue_zone = q2.revenue_zone 
        and q1.service_type = q2.service_type
        and q1.revenue_quarter = q2.revenue_quarter
        and q1.revenue_year = q2.revenue_year + 1
