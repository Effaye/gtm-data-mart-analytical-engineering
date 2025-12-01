{{
  config(
    materialized = 'table'
  )
}}

with source as (
    -- this is an conceptual model for date, data source omitted for brevity
    -- potentially can be derived from web_analytics, or from GTM planning tools (ex. Monday)
)

select
    campaign_id,
    channel_id,
    channel,
    utm_source,
    utm_campaign,
    purpose,                    -- e.g., new customer acquisition, retargeting, brand awareness
    target_audience_tags,       -- tags used to define target audience, if available
    targeted_list,              -- list of targeted users/groups, if available
    campaign_start_date::date,
    campaign_end_date::date,
    budget_usd::float

from source