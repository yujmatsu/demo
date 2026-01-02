-- 平均注文金額が総購入金額より大きくないことを確認するテスト
-- avg_order_value > total_revenue のレコードが存在する場合、このテストは失敗する

select
    customer_key,
    customer_name,
    total_orders,
    total_revenue,
    avg_order_value

from {{ ref('fct_customer_orders') }}

where avg_order_value > total_revenue
