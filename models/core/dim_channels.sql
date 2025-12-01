{{
  config(
    materialized = 'table'
  )
}}

with main_source as (
    -- potentially can be derived from web_analytics, or from GTM planning tools (ex. Monday)
    select distinct Proper(channel) as channel, 
    lower(utm_source) as utm_source,
    true as is_paid_media,
    from {{ ref('stg_ad_spend') }}
),

fallback_source as (
    
    -- adding channels from salesforce, in case they are not covered by the above
    select distinct Proper(source) as channel, lower(source) as utm_source
    false as is_paid_media
    from {{ ref('stg_salesforce_opportunities') }}
    where channel not in (select channel from main_source)
),

source as (
    select * from main_source
    union all
    select * from fallback_source
)



select 
    channel,
    utm_source,
    is_paid_media,
    -- if channel benchmarking data is available, can be added here
    -- e.g., chanel_benchmark_cpc

from source

-- example table output
--      channel	utm_source
-- 1	Google Ads	google
-- 2	Linkedin	linkedin
-- 3	Meta	facebook
-- 4	Twitter	twitter
-- 5	Direct	direct
-- 6	Organic	organic
-- 7	Partner	partner