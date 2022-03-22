{{
    config(
        materialized='incremental',
        unique_key='state'
    )
}}

select
    state as state,
    county as county,
    city as city,
    last_visit_date as last_visit_date
from {{ ref('seed') }}

{% if is_incremental() %}
    where last_visit_date > (select max(last_visit_date) from {{ this }})
{% endif %}