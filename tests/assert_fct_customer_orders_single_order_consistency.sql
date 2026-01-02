-- Test to ensure single-order customers have consistent date fields
-- When total_orders = 1, first_order_date should equal last_order_date
-- and customer_lifetime_days should be 0

select
    customer_key,
    customer_name,
    total_orders,
    first_order_date,
    last_order_date,
    customer_lifetime_days

from {{ ref('fct_customer_orders') }}

where total_orders = 1
  and (
      first_order_date != last_order_date
      or customer_lifetime_days != 0
  )
