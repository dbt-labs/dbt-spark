{{ config(
    materialized = 'table',
) }}

select * from {{ source('custom', 'users') }}