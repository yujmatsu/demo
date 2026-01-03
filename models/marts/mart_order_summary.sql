{{ config(
    materialized='table',
    tags=['mart', 'daily']
) }}

WITH order_base AS (
    SELECT
        DATE(order_date) AS order_date,
        order_id,
        customer_id,
        amount
    FROM {{ ref('stg_orders') }}
)

SELECT
    order_date,
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(DISTINCT customer_id) AS unique_customers,
    SUM(amount) AS total_revenue,
    AVG(amount) AS avg_order_value,
    MIN(amount) AS min_order_value,
    MAX(amount) AS max_order_value
FROM order_base
GROUP BY order_date
ORDER BY order_date DESC