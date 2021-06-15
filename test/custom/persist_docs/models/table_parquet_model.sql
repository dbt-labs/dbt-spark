{{ config(materialized='table', file_format='parquet', enabled=(target.name!='endpoint')) }}
select 1 as id, 'Joe' as name
