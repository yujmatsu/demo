{% macro generate_surrogate_key(columns) %}
    {#
    複数カラムからサロゲートキー（ハッシュ値）を生成するマクロ（Snowflake用）

    引数:
        columns (list): ハッシュ化するカラム名のリスト

    返り値:
        SQL式（hash関数の呼び出し）- NUMBER型

    仕様:
        - Snowflake の HASH() 関数を使用
        - NULL値は空文字 ('') に置換して一貫性を保つ
        - 各カラムを VARCHAR にキャストしてから結合
        - 結果は NUMBER 型の整数（負の値もあり得る）

    注意:
        - NUMBER型が扱いづらい場合は to_varchar() でラップ可能（下記参照）
        - 16進数文字列にしたい場合: to_varchar(hash(...), 'hex')

    使用例:
        select
            {{ generate_surrogate_key(['order_key', 'line_number']) }} as lineitem_key,
            order_key,
            line_number
        from {{ ref('stg_tpch__lineitem') }}

    別バージョン（VARCHAR型で出力）:
        {{ generate_surrogate_key_varchar(['order_key', 'line_number']) }}
    #}

    {%- set column_list = [] -%}
    {%- for column in columns -%}
        {%- do column_list.append("coalesce(cast(" ~ column ~ " as varchar), '')") -%}
    {%- endfor -%}

hash({{ column_list | join(', ') }})

{%- endmacro %}


{% macro generate_surrogate_key_varchar(columns, format='hex') %}
    {#
    複数カラムからサロゲートキー（ハッシュ値）を生成し、VARCHAR型で返すマクロ

    引数:
        columns (list): ハッシュ化するカラム名のリスト
        format (str): 出力フォーマット（'hex' または 'default'）デフォルトは 'hex'

    返り値:
        VARCHAR型のハッシュ値
        - 'hex': 16進数文字列（例: 'A1B2C3D4'）
        - 'default': 数値の文字列表現（例: '-123456789'）

    使用例:
        select
            {{ generate_surrogate_key_varchar(['order_key', 'line_number']) }} as lineitem_key,
            order_key,
            line_number
        from {{ ref('stg_tpch__lineitem') }}
    #}

    {%- set column_list = [] -%}
    {%- for column in columns -%}
        {%- do column_list.append("coalesce(cast(" ~ column ~ " as varchar), '')") -%}
    {%- endfor -%}

    {%- if format == 'hex' -%}
to_varchar(hash({{ column_list | join(', ') }}), 'hex')
    {%- else -%}
to_varchar(hash({{ column_list | join(', ') }}))
    {%- endif -%}

{%- endmacro %}
