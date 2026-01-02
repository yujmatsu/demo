{% macro generate_date_parts(date_column) %}
    {#
    日付カラムから年、四半期、月、週、曜日を生成する汎用マクロ（Snowflake用）

    引数:
        date_column (str): 日付カラム名（文字列）

    出力:
        カンマ区切りの SELECT 断片（複数カラム）
        - {date_column}_year: 年（数値）
        - {date_column}_quarter: 四半期（1-4）
        - {date_column}_month: 月（1-12）
        - {date_column}_week: 週番号（ISO week、1-53）
        - {date_column}_day_of_week: 曜日（ISO 1-7、月曜=1、日曜=7）
        - {date_column}_day_name: 曜日名（Monday, Tuesday, ...）

    使用例:
        select
            order_date,
            {{ generate_date_parts('order_date') }}
        from orders
    #}

year({{ date_column }}) as {{ date_column }}_year,
quarter({{ date_column }}) as {{ date_column }}_quarter,
month({{ date_column }}) as {{ date_column }}_month,
weekiso({{ date_column }}) as {{ date_column }}_week,
dayofweekiso({{ date_column }}) as {{ date_column }}_day_of_week,
dayname({{ date_column }}) as {{ date_column }}_day_name

{%- endmacro %}
