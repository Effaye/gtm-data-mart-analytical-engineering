# GTM Data Mart

This repository contains the analytic mart for sales channel efficiency, marketing strategy, and account prioritization.

## Structure
- `data/` — Raw and processed data files
- `models/` — ETL and transformation scripts
- `macros/` — dbt macros
- `config/` — Configuration files (e.g., database, environment)
- `tests/` — Test scripts for validation

## Main Features
- Channel funnel reporting
- Opportunity scoring
- Ideal customer profiling
- Sales and marketing efficiency analysis

## Architectural decisions and rationale 
This execise assumes the data warehouse choice is Snowflake/Bigquery, and is taking an 3-layer approach with staging (ingestion) > core > mart. 

Staging layer is the layer that consolidate transaction/event data from an individual data source (e.g., salesforce), and one row will represent one transaction/event that happened in that system - essentially the fact tables in the schema. If the data sources get more complicated, and will need to join a few tables to get all information needed for an event, then an extra intermediate layer (ingestiono layer) can be introduced for simple clean-ups (e.g., data format) of each source table before joining them in the staging layer. In this example, the ingestion layer is omitted as the data sources were already simplified and consolidated.

Core layer is the layer the consolidate one transaction/event data from multiple data source (e.g., web event linked to a salesforce opportunitiy). This tables might not be directly used in reporting, but the mapping and column definitions defined here is a major part of keeping single source of truth in an organization.

Mart layer is the layer that fuels reporting directly and might have special aggregations/structures to create desired visualizations in BI/reporting. The tables here are usually OBT (one big table) for a specific topic - as OBT is faster than the traiditional STAR schemas for columnar databases like Redshift/Bigquery/Snwoflake as suggested by fivetran: https://www.fivetran.com/blog/star-schema-vs-obt


![diagram](https://github.com/Effaye/gtm-data-mart-analytical-engineering/blob/main/diagram.png)

## Dashboard Example
![snapshot](https://github.com/Effaye/gtm-data-mart-analytical-engineering/blob/main/visuals%2Binsights/tableau%20dashboard.png)

🔗 Full interactive version:  
https://public.tableau.com/views/FunnelDash/Dashboard1
