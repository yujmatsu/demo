-- 最初の注文日が最後の注文日より後になっていないことを確認するテスト
-- first_order_date > last_order_date のレコードが存在する場合、このテストは失敗する

select
    customer_key,
    customer_name,
    first_order_date,
    last_order_date

from {{ ref('fct_customer_orders') }}

where first_order_date > last_order_date
