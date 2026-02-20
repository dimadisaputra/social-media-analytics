{{ config(materialized='table') }}

{{ dbt_date.get_date_dimension(
    start_date=var('dbt_date:start_date'),
    end_date=var('dbt_date:end_date')
) }}