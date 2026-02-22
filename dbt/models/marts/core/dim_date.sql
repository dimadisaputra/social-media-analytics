-- models/marts/core/dim_date.sql
-- Date dimension generated via dbt_date package
-- Configure start_date and end_date in dbt_project.yml under vars:
--   dbt_date:start_date: '2020-01-01'
--   dbt_date:end_date:   '2030-12-31'

{{ config(materialized='table') }}

{{ dbt_date.get_date_dimension(
    start_date=var('dbt_date:start_date'),
    end_date=var('dbt_date:end_date')
) }}