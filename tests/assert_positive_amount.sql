-- 注文金額が負でないことを検証
select
  order_id,
  amount
from {{ ref('stg_orders') }}
where amount < 0