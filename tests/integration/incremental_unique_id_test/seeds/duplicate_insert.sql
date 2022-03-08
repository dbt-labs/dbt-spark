-- insert new row, which should not be in incremental model
--  with primary or first three columns unique
insert into {schema}.seed
    (state, county, city, last_visit_date)
values ('CT','Hartford','Hartford','2022-02-14');