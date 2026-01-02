-- Test to ensure critical customer attributes are not null
-- customer_name, market_segment, and nation_name should always have values

select
    customer_key,
    customer_name,
    market_segment,
    nation_name

from {{ ref('fct_customer_orders') }}

where customer_name is null
   or market_segment is null
   or nation_name is null
