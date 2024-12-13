{{
    config(database = 'employees_final',
        materialized='table')
}}

with
    source as (select * from {{ source("employees_data", "employees") }})
select * from source