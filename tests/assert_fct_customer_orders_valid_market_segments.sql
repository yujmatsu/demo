-- Test to ensure market_segment contains only valid values
-- Valid values: AUTOMOBILE, BUILDING, FURNITURE, MACHINERY, HOUSEHOLD

select
    customer_key,
    customer_name,
    market_segment

from {{ ref('fct_customer_orders') }}

where market_segment not in ('AUTOMOBILE', 'BUILDING', 'FURNITURE', 'MACHINERY', 'HOUSEHOLD')
