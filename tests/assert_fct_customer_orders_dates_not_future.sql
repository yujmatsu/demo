-- Test to ensure order dates are not in the future
-- first_order_date and last_order_date should not be greater than current date

select
    customer_key,
    customer_name,
    first_order_date,
    last_order_date,
    current_date() as today

from {{ ref('fct_customer_orders') }}

where first_order_date > current_date()
   or last_order_date > current_date()
