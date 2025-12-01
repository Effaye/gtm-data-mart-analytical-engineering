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
    date||'_'||utm_source||'_'||utm_campaign||'_'||campaign_id as ad_spend_uid,
    date::date as date,
    campaign_id,
    utm_source,
    utm_campaign,
    sum(spend_usd::float) as spend_usd,
    sum(clicks::int) as clicks,
    sum(impressions::int) as impressions

from source
group by all