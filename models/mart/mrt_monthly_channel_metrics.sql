{{
  config(
    materialized = 'table'
  )
}}

with mrt_channel_funnel as (select * from {{ ref('mrt_channel_funnel') }}),

channel_monthly_summary as (
select 
  channel, 
  date_trunc(date_day, month) as month,
  sum(total_spend_usd) as total_spend_usd,
  sum(total_clicks) as total_clicks,
  sum(total_impressions) as total_impressions,
  sum(sessions) as total_sessions,
  --   excluding non-paid media sessions to calculate cost efficiency metrics
  sum(case when is_paid_media then sessions end) as total_paid_media_sessions,
  sum(converted_sessions) as total_converted_sessions,
  sum(total_opportunities) as total_opportunities,
  sum(closed_won_opportunities) as total_closed_won_opportunities,
  sum(total_revenue_usd) as total_revenue_usd,
  --   excluding non-paid media sessions to calculate cost efficiency metrics
  sum(case when is_paid_media then total_revenue_usd end) as total_paid_media_revenue_usd
  from mrt_channel_funnel
  group by all

  union all

select 
  'Total' as channel, 
  date_trunc(date_day, month) as month,
  sum(total_spend_usd) as total_spend_usd,
  sum(total_clicks) as total_clicks,
  sum(total_impressions) as total_impressions,
  sum(sessions) as total_sessions,
  --   excluding non-paid media sessions to calculate cost efficiency metrics
  sum(case when is_paid_media then sessions end) as total_paid_media_sessions,
  sum(converted_sessions) as total_converted_sessions,
  sum(total_opportunities) as total_opportunities,
  sum(closed_won_opportunities) as total_closed_won_opportunities,
  sum(total_revenue_usd) as total_revenue_usd,
  --   excluding non-paid media sessions to calculate cost efficiency metrics
  sum(case when is_paid_media then total_revenue_usd end) as total_paid_media_revenue_usd
  from mrt_channel_funnel
  group by all
),

-- defining the funnel metrics to calculate, this can also be stored 
-- as a separate dimension table, or a dbt seed file
gtm_funnel_metrics as (
    SELECT *
    FROM (
        VALUES
            (1, 1, 'ads', 'total impression', 'sum'),
            (2, 1, 'ads', 'total clicks', 'sum'),
            (3, 1, 'ads', 'total ad spend', 'sum'),
            (4, 1, 'ads', 'cost per click', 'ratio'),
            (5, 1, 'ads', 'ad click rate%', 'ratio'),
            (6, 2, 'site', 'total sessions', 'sum'),
            (7, 2, 'site', 'total form submissions', 'sum'),
            (8, 2, 'site', 'site conversion rate', 'ratio'),
            (9, 2, 'site', 'cost per session', 'ratio'),
            (10, 3, 'salesforce', 'total opportunities', 'sum'),
            (11, 3, 'salesforce', 'total closed won opportunities', 'sum'),
            (12, 3, 'salesforce', 'close won rate', 'ratio'),
            (13, 3, 'salesforce', 'total revenue', 'sum'),
            (14, 3, 'salesforce', 'roas', 'ratio')
    ) AS s (index, stage, stage_name, metric_name, aggregation_method)
)


select 
channel, month, stage, stage_name, index, metric_name, aggregation_method,
-- the metric calculations can also be stored in a yaml file + macro for better maintainability
case 
  when metric_name = 'total impression' then  total_impressions 
  when metric_name = 'total clicks' then total_clicks 
  when metric_name = 'total ad spend' then total_spend_usd
  when metric_name = 'cost per click' then  total_spend_usd
  when metric_name = 'ad click rate%' then total_clicks
  when metric_name = 'total sessions' then total_sessions
  when metric_name = 'total form submissions' then total_converted_sessions
  when metric_name = 'site conversion rate' then total_converted_sessions
  when metric_name = 'cost per session' then total_spend_usd
  when metric_name = 'total opportunities' then channel_summary.total_opportunities
  when metric_name = 'total closed won opportunities' then total_closed_won_opportunities
  when metric_name = 'close won rate' then total_closed_won_opportunities
  when metric_name = 'total revenue' then channel_summary.total_revenue_usd
  when metric_name = 'roas' then channel_summary.total_paid_media_revenue_usd
 end as value,

 case 
  when metric_name = 'cost per click' then  total_clicks
  when metric_name = 'ad click rate%' then total_impressions
  when metric_name = 'site conversion rate' then total_sessions
  when metric_name = 'cost per session' then total_paid_media_sessions
  when metric_name = 'close won rate' then total_opportunities
  when metric_name = 'roas' then total_spend_usd
 end as devided_by,

-- in BI tool, the all metric's value can be calculated using
-- if aggregation_method = 'sum' then sum(value)
-- if aggregation_method = 'ratio' then sum(value) / sum(devided_by)    
-- to prevent duplicated workload in BI tool for creating calculations for each metric.
-- see example in /visuals

from channel_summary, gtm_funnel_metrics