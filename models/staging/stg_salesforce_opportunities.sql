{{
  config(
    materialized = 'view'
  )
}}

with source as (

    select * 
    from {{ source('raw', 'salesforce_opportunities') }}

)

select
    opportunity_id,
    account_id,
    created_date::date as created_date,
    stage,
    amount_usd::float,
    source,
    case 
        when source = 'Google Ads' then 'google'
        when source = 'LinkedIn' then 'linkedin'
        when source = 'Meta' then 'facebook'
        when source = 'Twitter' then 'twitter'  
    end as utm_source,
    owner_region

from source