{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    file_format = 'delta',
    unique_key = 'id',
    incremental_predicates = [
        "dbt_internal_dest.id != 2"
    ],
) }}

{% if not is_incremental() %}

select cast(1 as bigint) as id, 'hello' as msg, 'blue' as color
union all
select cast(2 as bigint) as id, 'goodbye' as msg, 'red' as color

{% else %}

-- merge will not happen on the above record where id = 2, so new record will fall to insert
select cast(1 as bigint) as id, 'hey' as msg, 'blue' as color
union all
select cast(2 as bigint) as id, 'yo' as msg, 'green' as color
union all
select cast(3 as bigint) as id, 'anyway' as msg, 'purple' as color

{% endif %}
