# fct_customer_orders 追加テスト提案

## データプロファイリング実施手順

1. 以下のコマンドでプロファイリングSQLをコンパイル:
```bash
dbt compile --select fct_customer_orders_profiling
```

2. `target/compiled/demo/analysis/fct_customer_orders_profiling.sql` を Snowflake で実行

## 推奨される追加テスト

### 1. 計算精度の検証

**テスト名**: `assert_fct_customer_orders_avg_order_value_accuracy.sql`
**目的**: avg_order_value が total_revenue / total_orders と一致することを確認
**優先度**: 高
**理由**: 計算ロジックの正確性を保証

### 2. 顧客属性のNULL検証

**テスト名**: `assert_fct_customer_orders_customer_attributes_not_null.sql`
**目的**: customer_name, market_segment, nation_name にNULLがないことを確認
**優先度**: 高
**理由**: 顧客分析に必須の属性が欠損していると分析精度が低下

### 3. 日付の妥当性検証

**テスト名**: `assert_fct_customer_orders_dates_not_future.sql`
**目的**: first_order_date と last_order_date が未来の日付でないことを確認
**優先度**: 中
**理由**: 未来の注文日は明らかなデータエラー

### 4. market_segment の値の制約

**テスト名**: `assert_fct_customer_orders_valid_market_segments.sql`
**目的**: market_segment が有効な値のみ（AUTOMOBILE, BUILDING, FURNITURE, MACHINERY, HOUSEHOLD）であることを確認
**優先度**: 中
**理由**: 不正な値が含まれると分析結果に影響

### 5. 単一注文顧客の整合性

**テスト名**: `assert_fct_customer_orders_single_order_consistency.sql`
**目的**: total_orders = 1 の場合、first_order_date = last_order_date かつ customer_lifetime_days = 0 であることを確認
**優先度**: 中
**理由**: 単一注文顧客の特殊ケースの整合性

### 6. avg_order_value と total_revenue の関係

**テスト名**: `assert_fct_customer_orders_avg_revenue_relationship.sql`
**目的**: avg_order_value <= total_revenue であることを確認（既存テストと重複の可能性）
**優先度**: 低
**理由**: 既に similar なテストが存在する可能性

### 7. 異常値の検出

**テスト名**: `assert_fct_customer_orders_reasonable_ranges.sql`
**目的**: total_revenue や total_orders が異常に大きくないことを確認（例: total_orders > 10000）
**優先度**: 低
**理由**: ビジネスロジックに基づく妥当性チェック（閾値はビジネス要件による）

## 実装推奨順序

1. 計算精度の検証（優先度: 高）
2. 顧客属性のNULL検証（優先度: 高）
3. 日付の妥当性検証（優先度: 中）
4. market_segment の値の制約（優先度: 中）
5. 単一注文顧客の整合性（優先度: 中）

## 次のステップ

1. `analysis/fct_customer_orders_profiling.sql` を実行して実際のデータ特性を確認
2. プロファイリング結果に基づいて上記テストの優先順位を調整
3. 優先度の高いテストから実装
4. テストを定期的に実行し、データ品質をモニタリング
