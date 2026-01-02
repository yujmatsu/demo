{{
    config(
        materialized='incremental',
        unique_key='customer_key',
        on_schema_change='sync_all_columns',
        tags=['incremental', 'customer_lifetime']
    )
}}

with orders_with_customer as (

    select * from {{ ref('int_orders__with_customer') }}

),

{% if is_incremental() %}

-- 直近7日で変化があった顧客を抽出（late-arriving data 対応）
changed_customers as (

    select distinct customer_key
    from orders_with_customer
    where order_date >= dateadd(day, -7, current_date)

),

-- 変化があった顧客の全期間注文データを取得
orders_to_recalculate as (

    select o.*
    from orders_with_customer o
    inner join changed_customers c
        on o.customer_key = c.customer_key

),

{% endif %}

customer_orders_summary as (

    select
        customer_key,

        -- Customer attributes (one per customer)
        max(customer_name) as customer_name,
        max(market_segment) as market_segment,
        max(nation_name) as nation_name,

        -- Order aggregations (全期間の集計)
        count(order_key) as total_orders,
        sum(total_price) as total_revenue,
        avg(total_price) as avg_order_value,

        -- Date aggregations
        min(order_date) as first_order_date,
        max(order_date) as last_order_date,
        datediff(day, min(order_date), max(order_date)) as customer_lifetime_days

    from {% if is_incremental() %}
        orders_to_recalculate
    {% else %}
        orders_with_customer
    {% endif %}
    group by customer_key

)

select * from customer_orders_summary
