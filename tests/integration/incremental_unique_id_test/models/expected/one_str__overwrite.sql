{{
    config(
        materialized='table'
    )
}}

select
    'CT' as state,
    'Hartford' as county,
    'Hartford' as city,
    cast('2022-02-14' as date) as last_visit_date
union all
select 'MA','Suffolk','Boston','2020-02-12'
union all
select 'NJ','Mercer','Trenton','2022-01-01'
union all
select 'NY','Kings','Brooklyn','2021-04-02'
union all
select 'NY','New York','Manhattan','2021-04-01'
union all
select 'PA','Philadelphia','Philadelphia','2021-05-21'