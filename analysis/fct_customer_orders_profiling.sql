-- Data profiling analysis for fct_customer_orders
-- Run this with: dbt compile --select fct_customer_orders_profiling
-- Then execute the compiled SQL in Snowflake

-- 1. Basic statistics
SELECT
    'Basic Statistics' as analysis_type,
    COUNT(*) as total_records,
    COUNT(DISTINCT customer_key) as unique_customers,
    MIN(total_orders) as min_orders,
    MAX(total_orders) as max_orders,
    AVG(total_orders)::DECIMAL(10,2) as avg_orders,
    MIN(total_revenue) as min_revenue,
    MAX(total_revenue) as max_revenue,
    AVG(total_revenue)::DECIMAL(10,2) as avg_revenue,
    MIN(customer_lifetime_days) as min_lifetime_days,
    MAX(customer_lifetime_days) as max_lifetime_days,
    AVG(customer_lifetime_days)::DECIMAL(10,2) as avg_lifetime_days
FROM {{ ref('fct_customer_orders') }};

-- 2. NULL checks
SELECT
    'NULL Checks' as analysis_type,
    SUM(CASE WHEN customer_key IS NULL THEN 1 ELSE 0 END) as null_customer_key,
    SUM(CASE WHEN customer_name IS NULL THEN 1 ELSE 0 END) as null_customer_name,
    SUM(CASE WHEN market_segment IS NULL THEN 1 ELSE 0 END) as null_market_segment,
    SUM(CASE WHEN nation_name IS NULL THEN 1 ELSE 0 END) as null_nation_name,
    SUM(CASE WHEN total_orders IS NULL THEN 1 ELSE 0 END) as null_total_orders,
    SUM(CASE WHEN total_revenue IS NULL THEN 1 ELSE 0 END) as null_total_revenue,
    SUM(CASE WHEN avg_order_value IS NULL THEN 1 ELSE 0 END) as null_avg_order_value,
    SUM(CASE WHEN first_order_date IS NULL THEN 1 ELSE 0 END) as null_first_order_date,
    SUM(CASE WHEN last_order_date IS NULL THEN 1 ELSE 0 END) as null_last_order_date
FROM {{ ref('fct_customer_orders') }};

-- 3. Calculation accuracy check
SELECT
    'Calculation Accuracy' as analysis_type,
    COUNT(*) as total_records,
    SUM(CASE
        WHEN ABS(avg_order_value - (total_revenue / total_orders)) > 0.01
        THEN 1 ELSE 0
    END) as avg_order_value_mismatch,
    SUM(CASE
        WHEN customer_lifetime_days != DATEDIFF(day, first_order_date, last_order_date)
        THEN 1 ELSE 0
    END) as lifetime_days_mismatch
FROM {{ ref('fct_customer_orders') }};

-- 4. Negative value checks
SELECT
    'Negative Values' as analysis_type,
    SUM(CASE WHEN total_orders < 0 THEN 1 ELSE 0 END) as negative_orders,
    SUM(CASE WHEN total_revenue < 0 THEN 1 ELSE 0 END) as negative_revenue,
    SUM(CASE WHEN avg_order_value < 0 THEN 1 ELSE 0 END) as negative_avg,
    SUM(CASE WHEN customer_lifetime_days < 0 THEN 1 ELSE 0 END) as negative_lifetime
FROM {{ ref('fct_customer_orders') }};

-- 5. Market segment distribution
SELECT
    'Market Segment Distribution' as analysis_type,
    market_segment,
    COUNT(*) as customer_count,
    SUM(total_revenue) as segment_revenue,
    AVG(total_orders)::DECIMAL(10,2) as avg_orders_per_customer
FROM {{ ref('fct_customer_orders') }}
GROUP BY market_segment
ORDER BY customer_count DESC;

-- 6. Outlier detection (values beyond 3 standard deviations)
WITH stats AS (
    SELECT
        AVG(total_revenue) as avg_revenue,
        STDDEV(total_revenue) as stddev_revenue,
        AVG(total_orders) as avg_orders,
        STDDEV(total_orders) as stddev_orders
    FROM {{ ref('fct_customer_orders') }}
)
SELECT
    'Outliers' as analysis_type,
    COUNT(*) as total_outliers,
    SUM(CASE
        WHEN f.total_revenue > s.avg_revenue + (3 * s.stddev_revenue)
        THEN 1 ELSE 0
    END) as high_revenue_outliers,
    SUM(CASE
        WHEN f.total_orders > s.avg_orders + (3 * s.stddev_orders)
        THEN 1 ELSE 0
    END) as high_order_outliers
FROM {{ ref('fct_customer_orders') }} f
CROSS JOIN stats s;
