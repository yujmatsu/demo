with orders as (

    select * from {{ ref('stg_tpch__orders') }}

),

customers as (

    select * from {{ ref('stg_tpch__customer') }}

),

nations as (

    select * from {{ ref('stg_tpch__nation') }}

),

joined as (

    select
        -- Order information
        orders.order_key,
        orders.customer_key,
        orders.order_status,
        orders.total_price,
        orders.order_date,
        orders.order_priority,
        orders.clerk,
        orders.ship_priority,
        orders.comment as order_comment,

        -- Customer information
        customers.customer_name,
        customers.market_segment,
        customers.account_balance,

        -- Nation information
        nations.nation_name,
        nations.region_key

    from orders
    left join customers
        on orders.customer_key = customers.customer_key
    left join nations
        on customers.nation_key = nations.nation_key

)

select * from joined
