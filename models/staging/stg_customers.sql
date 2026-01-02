{{ config(materialized='view') }}

SELECT
    customer_id,
    customer_name,
    email,
    region
FROM {{ source('raw_data', 'raw_customers') }}