{{ config(materialized='table') }}

SELECT
    c.customer_id,
    c.customer_name,
    c.region,
    COUNT(o.order_id) AS order_count,
    COALESCE(SUM(o.amount), 0) AS total_amount,
    AVG(o.amount) AS avg_amount
FROM {{ ref('stg_customers') }} c
    LEFT JOIN {{ ref('stg_orders') }} o
        ON c.customer_id = o.customer_id
        AND o.status = 'completed'  -- 完了した注文のみ
GROUP BY c.customer_id, c.customer_name, c.region
