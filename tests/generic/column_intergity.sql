{% test column_intergity(model, column_name, compare_model, compare_column, condition1=none, condition2=none, is_distinct=False) %}
{#-- Test column intergity between source and base conversion. Detect error in casting-- #}
{#-- Requires to provide the source name / column name, condition/ distinct check are applicable. --#}
{{ config(fail_calc = 'coalesce(diff_count, 0)') }}

{#-- Prevent querying of db in parsing mode. This works because this macro does not create any new refs. #}
{%- if not execute -%}
    {{ return('') }}
{% endif %}

with a as (

    select count({% if is_distinct %} distinct {% endif %} {{ column_name }}) as count_a from {{ model }}
    {% if condition1 is not none %}
    {{ condition1 }}
    {% endif %}
),
b as (

    select count({% if is_distinct %} distinct {% endif %} {{ compare_column }}) as count_b from {{ compare_model }}
    {% if condition2 is not none %}
    {{ condition2 }}
    {% endif %}
),
final as (

    select
        count_a,
        count_b,
        abs(count_a - count_b) as diff_count
    from a
    cross join b

)

select * from final

{% endtest %}