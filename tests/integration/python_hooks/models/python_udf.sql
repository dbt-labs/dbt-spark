{{ config(
    materialized = 'table',
) }}

select 3L as x, squaredWithPython(3) as squared