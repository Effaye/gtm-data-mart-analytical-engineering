{{
  config(
    materialized = 'view'
  )
}}

with source as (

    select * 
    from {{ source('raw', 'web_analytics') }}

)

select
    session_id,
    user_id,
    session_date::date as session_date,
    landing_page,
    utm_source,
    utm_campaign,
    pageviews::int as pageviews,
    conversions::int as conversions,
    case when conversions::int > 0 then true else false end as is_converted

from source