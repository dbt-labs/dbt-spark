{{
    config(
        materialized='incremental',
        unique_key=['state', 'thisisnotacolumn']
    )
}}

select * from {{ ref('seed') }}