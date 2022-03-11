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
select 'MA','Suffolk','Boston',cast('2020-02-12' as date)
union all
select 'NJ','Mercer','Trenton',cast('2022-01-01' as date)
union all
select 'NY','Kings','Brooklyn',cast('2021-04-02' as date)
union all
select 'NY','New York','Manhattan',cast('2021-04-01' as date)
union all
select 'PA','Philadelphia','Philadelphia',cast('2021-05-21' as date)