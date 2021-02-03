{{ config(
    materialized = 'incremental',
) }}

{% if not is_incremental() %}

select 1 as id, 'hello' as msg
union all
select 2 as id, 'goodbye' as msg

{% else %}

select 2 as id, 'yo' as msg
union all
select 3 as id, 'anyway' as msg

{% endif %}
