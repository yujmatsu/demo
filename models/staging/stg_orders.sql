{{ config(materialized='view') }}

SELECT
    order_id,
    customer_id,
    order_date,
    amount,
    status
FROM {{ source('raw_data', 'raw_orders') }}
WHERE status IN ('completed', 'pending', 'cancelled')
