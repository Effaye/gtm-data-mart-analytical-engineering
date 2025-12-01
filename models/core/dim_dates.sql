{{
  config(
    materialized = 'table'
  )
}}

with date_source as (
    -- this is an conceptual model for date, data source omitted for brevity
)

select 

    date::date as day_date,
    extract(year from day_date) as year,
    extract(month from day_date) as month,
    datetrunc('week', day_date) as week_start_date,
    datetrunc('month', day_date) as month_start_date,
    case when day_date = week_start_date then true else false end as is_week_start,
    case when 
    is_business_day,
    is_holiday,
    holiday_name

from date_source