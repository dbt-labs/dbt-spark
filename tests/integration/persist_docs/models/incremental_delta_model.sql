{{ config(materialized='incremental', file_format='delta') }}
select 1 as id, 'Joe' as name
