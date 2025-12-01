{{
  config(
    materialized = 'table'
  )
}}


with 
-- listing all referenced models upfront
dates as (select * from {{ ref('dim_dates') }}),
channels as (select * from {{ ref('dim_channels') }}),
web_analytics as (select * from {{ ref('stg_web_analytics') }}),
ad_spend as (select * from {{ ref('stg_daily_campaign_ad_spend') }}),
salesforce_opportunities as (select * from {{ ref('stg_salesforce_opportunities') }}),

-- CTE for aggregations
daily_channel_spend as (

    select
        date as date_day,
        utm_source,
        sum(spend_usd) as total_spend_usd,
        sum(clicks) as total_clicks,
        sum(impressions) as total_impressions

    from ad_spend
    group by all
),

daily_web_analytics_by_channel as (

    select
        session_date as date_day,
        coalesce(utm_source, 'direct') as utm_source, -- treating null utm_source as 'direct'
        count(distinct session_id) as sessions,
        count(distinct case when is_converted then session_id end) as converted_sessions,

    from web_analytics
    group by all
),

daily_salesforce_opportunities_by_channel as (

    select
        created_date as date_day,
        source as channel,
        count(distinct opportunity_id) as total_opportunities,
        count(distinct case when stage = 'Closed Won' then opportunity_id end) as closed_won_opportunities,
        sum(case when stage = 'Closed Won' then amount_usd end) as total_revenue_usd

    from salesforce_opportunities
    group by all
)
    
select
date_day,
channel,
utm_source,
coalesce(total_spend_usd, 0) as total_spend_usd,
coalesce(total_clicks, 0) as total_clicks,
coalesce(total_impressions, 0) as total_impressions,
coalesce(sessions, 0) as sessions,
coalesce(converted_sessions, 0) as converted_sessions,
coalesce(total_opportunities, 0) as total_opportunities,
coalesce(closed_won_opportunities, 0) as closed_won_opportunities,
coalesce(total_revenue_usd, 0) as total_revenue_usd


from dates, channels
left join daily_channel_spend using (date_day, utm_source)
left join daily_web_analytics_by_channel using (date_day, utm_source)
left join daily_salesforce_opportunities_by_channel using(date_day, channel)