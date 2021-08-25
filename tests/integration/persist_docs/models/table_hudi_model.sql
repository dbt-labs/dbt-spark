{{ config(materialized='table', file_format='hudi') }}
select 1 as id, 'Vino' as name
