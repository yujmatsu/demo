-- 注文件数と総購入金額のデータ整合性を確認するテスト
-- total_orders = 0 だが total_revenue != 0 のレコードが存在する場合、このテストは失敗する

select
    customer_key,
    customer_name,
    total_orders,
    total_revenue

from {{ ref('fct_customer_orders') }}

where total_orders = 0
  and total_revenue != 0
