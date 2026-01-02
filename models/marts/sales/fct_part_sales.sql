{{
    config(
        materialized='table',
        tags=['marts', 'sales', 'part_analytics']
    )
}}

with lineitem as (

    select * from {{ ref('stg_tpch__lineitem') }}

),

part as (

    select * from {{ ref('stg_tpch__part') }}

),

part_sales_summary as (

    select
        lineitem.part_key,

        -- 商品属性（部品マスタから取得）
        part.part_name,
        part.manufacturer,
        part.brand,
        part.part_type,
        part.size,
        part.retail_price,

        -- 集計指標
        -- 総販売数量: sum(quantity)
        sum(lineitem.quantity) as total_quantity,

        -- 総売上金額: sum(extended_price * (1 - discount) * (1 + tax))
        -- 割引後・税込みの最終売上金額
        sum(
            lineitem.extended_price
            * (1 - lineitem.discount)
            * (1 + lineitem.tax)
        ) as total_revenue,

        -- 平均単価: total_revenue / nullif(total_quantity, 0)
        -- ゼロ除算を回避するため nullif を使用
        sum(
            lineitem.extended_price
            * (1 - lineitem.discount)
            * (1 + lineitem.tax)
        ) / nullif(sum(lineitem.quantity), 0) as avg_unit_price,

        -- 総注文数: count(distinct order_key)
        count(distinct lineitem.order_key) as total_orders,

        -- 参考指標（割引前の売上）
        sum(lineitem.extended_price) as total_revenue_before_discount,

        -- 参考指標（平均割引率）
        avg(lineitem.discount) as avg_discount_rate,

        -- 参考指標（平均税率）
        avg(lineitem.tax) as avg_tax_rate

    from lineitem
    left join part
        on lineitem.part_key = part.part_key

    group by
        lineitem.part_key,
        part.part_name,
        part.manufacturer,
        part.brand,
        part.part_type,
        part.size,
        part.retail_price

)

select * from part_sales_summary
