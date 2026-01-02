-- Test to ensure avg_order_value is accurately calculated
-- avg_order_value should equal total_revenue / total_orders
-- Allow for small floating point differences (0.01)

select
    customer_key,
    customer_name,
    total_orders,
    total_revenue,
    avg_order_value,
    (total_revenue / total_orders) as calculated_avg,
    abs(avg_order_value - (total_revenue / total_orders)) as difference

from {{ ref('fct_customer_orders') }}

where abs(avg_order_value - (total_revenue / total_orders)) > 0.01
