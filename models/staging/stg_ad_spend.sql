{{
  config(
    materialized = 'view'
  )
}}

with source as (

    select * 
    from {{ source('raw', 'ad_spend') }}

)

select
    date::date as date,
    campaign_id,
    channel,
    utm_source,
    utm_campaign,
    spend_usd::float as spend_usd,
    clicks::int as clicks,
    impressions::int as impressions

from source