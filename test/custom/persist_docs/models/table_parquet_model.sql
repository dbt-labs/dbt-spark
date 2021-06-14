{{ config(materialized='table', file_format='parquet') }}
select 1 as id, 'Joe' as name
