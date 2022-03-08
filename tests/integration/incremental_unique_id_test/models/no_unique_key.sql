{{
    config(
        materialized='incremental'
    )
}}

select
    *
from {{ ref('seed') }}

{% if is_incremental() %}
    where last_visit_date > (select max(last_visit_date) from {{ this }})
{% endif %}