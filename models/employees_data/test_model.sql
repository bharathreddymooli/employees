{{ config(
    materialized='incremental', 
    unique_key='id',  
    incremental_strategy='merge',  
    tags=["incremental"]
) }}

WITH source AS (
    SELECT * 
    FROM {{ source('employees_data', 'employees') }}
)

MERGE INTO {{ this }} AS target
USING source
ON target.id = source.id 

-- Update existing records when there are differences in any field
WHEN MATCHED AND (
    target.name != source.name OR 
    target.age != source.age OR 
    target.department != source.department OR 
    target.salary != source.salary
) THEN
    UPDATE SET
        target.name = source.name, 
        target.age = source.age,
        target.department = source.department,
        target.salary = source.salary,
        target.updated_on = current_timestamp()

-- Insert new records that don't exist in the target
WHEN NOT MATCHED THEN
    INSERT (id, name, age, department, salary, updated_on)
    VALUES (source.id, source.name, source.age, source.department, source.salary, current_timestamp());
