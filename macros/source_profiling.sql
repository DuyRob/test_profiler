{% macro source_profiling(
    source_name,
    table_name,
    check_na=True,
    date_incremental=None,
    condition=None
) %}
{{
    return(
        adapter.dispatch("source_profiling")(
            source_name, table_name, check_na, date_incremental, condition
        )
    )
}}
{% endmacro %}

{% macro bigquery__source_profiling(
    source_name, table_name, check_na, date_incremental, condition
) %}
{% set source_relation = source(source_name, table_name) %}
{% set columns = adapter.get_columns_in_relation(source_relation) %}

{% if execute %}

{% set create_table_query %}
{% if date_incremental is not none %}
{% raw %}
{{ config(
    materialized = 'incremental', 
    unique_key = 'column_key', 
    on_schema_change = 'sync_all_columns', 
    partition_by = {'field':'profiled_at'},
    tags =[ "profiling" ]
 )}} 
{% endraw %}
{% endif %}
with prep as (
    select *,         
    'dummy' as partition_field
     from {% raw %}{{ source({% endraw %}'{{ source_name }}', '{{ table_name }}'{% raw %}) }}{% endraw %}
where 1=1 
{%if condition is not none%}
and {{condition}}
{% endif %}
{% if date_incremental is not none %}
{% raw %}{{ date_incremental({% endraw %}'{{ date_incremental}}', 1{% raw %}) }}{% endraw %}
{% endif %}

),

calculation_cte as (  
     
{% for column in columns %} 
     select distinct
       '{{column.name}}' as column_name,
       '{{column.dtype}}' as data_type,
        current_date() as profiling_date,
        COUNT( {{ column.name }}) as count, 
        COUNT(DISTINCT {{ column.name }}) as count_distinct,
        COUNTIF({{ column.name }} IS NULL) as count_null,
        IEEE_DIVIDE(COUNTIF({{ column.name }} IS NULL), COUNT( {{ column.name }} )) as null_ratio, 
        IEEE_DIVIDE(COUNT(DISTINCT {{ column.name }} ), COUNT( {{ column.name }})) as distinct_ratio, 
        {%if check_na %}
           {%- if column.dtype in ('STRING') -%}
        COUNTIF( lower({{ column.name }}) in ('nan','n/a','-infinty','na','','-','__','--','_'))) as count_na, 
           {%- else -%}
        null as count_na,
           {%- endif -%}
        {% endif %}
        {% if column.dtype in ('INT64','FLOAT64','NUMERIC','BIGNUMERIC') %}
        AVG({{ column.name }}) as average,
        SUM({{ column.name }}) as sum,
        STDDEV({{ column.name }}) as  standard_dev,
        MIN({{ column.name }}) OVER (PARTITION BY partition_field)  as min,
        MAX ({{ column.name }}) OVER (PARTITION BY partition_field)  as max
        {% else %}
        null as average,                           
        null as sum,
        null as standard_dev,
        null as min,
        null as max
        {% endif %}
     from prep 
     group by 1,2,3
    {{"union all " if not loop.last}}
   
{% endfor %} 
),

quantile_cte as (
    {% for column in columns %} 
     select distinct
       '{{column.name}}' as column_name,
       '{{column.dtype}}' as data_type,
        PERCENTILE_CONT ({{ column.name }},0.25) OVER(PARTITION BY partition_field ) as quantile_25,
        PERCENTILE_CONT ({{ column.name }},0.5) OVER(PARTITION BY partition_field ) as quantile_50,
        PERCENTILE_CONT ({{ column.name }},0.75) OVER(PARTITION BY partition_field )  as quantile_75,
        PERCENTILE_CONT ({{ column.name }},0.9) OVER(PARTITION BY partition_field )  as quantile_90,
        from prep
       {{"union all " if not loop.last}}
    {% endfor %}
)
                     
select  *,
{{dbt_utils.generate_surrogate_key (['column_name','data_type','profliling_date']) }} as column_key 
 from calculation_cte   
left join quantile_cte on 
calculation_cte.column_name = quantile_cte.column_name
and calculation_cte.data_type = quantile_cte.data_type
{% endset %}

{{ log(create_table_query, info=True) }}

{% endif %}
{% endmacro %}


{% macro snowflake__source_profiling(
    source_name, table_name, date_incremental, check_na, condition
) %}
{% set source_relation = source(source_name, table_name) %}
{% set columns = adapter.get_columns_in_relation(source_relation) %}
{% if execute %}
{% set create_table_query %}

            with prep as (
                select *                    
                from {% raw %}{{ source({% endraw %}'{{ source_name }}', '{{ table_name }}'{% raw %}) }}{% endraw %}
                where 1=1 
                {%if condition is not none%}
                and {{condition}}
                {% endif %}
                {% if date_incremental is not none %}
                and {% raw %}{{ date_incremental({% endraw %}'{{ date_incremental}}', 1{% raw %}) }}{% endraw %}
                {% endif %}          
                ),
                calculation_cte as (
                  
                
                {% for column in columns %}
                        select 
                        '{{column.name}}' as column_name,
                        '{{column.dtype}}' as data_type,
                        current_date() as profiling_date,
                        COUNT( {{ column.name }})  as count, 
                        COUNT(DISTINCT {{ column.name }}) as  count_distinct,
                        COUNT_IF({{ column.name }} IS NULL)  as count_null,
                        {% if check_na %}
                             {%- if column.dtype in ('STRING', 'VARCHAR','CHAR','CHARACTER','TEXT') -%}
                         COUNTIF( lower({{ column.name }}) in ('nan','n/a','-infinty','na','','-','__','--','_'))) as count_na, 
                             {%- else -%}
                        null as count_na, 
                             {%- endif -%}
                        {% endif %}
                        {% if column.dtype in('INT','NUMBER','DECIMAL','NUMERIC','BIGNUMERIC','FLOAT','DOUBLE') %}
                        AVG({{ column.name }})  as average,
                        SUM({{ column.name }}) as sum,
                        STDDEV({{ column.name }}) as  standard_dev,
                        MIN({{ column.name }})  as min,
                        APPROX_PERCENTILE( {{ column.name }}, 25) as quantile_25,
                        APPROX_PERCENTILE( {{ column.name }}, 50) as quantile_50,
                        APPROX_PERCENTILE( {{ column.name }}, 75) as quantile_75,
                        APPROX_PERCENTILE( {{ column.name }}, 90) as quantile_90,
                        MAX ({{ column.name }}) as max
                        {% else %}
                        null as average,                           
                        null as sum,
                        null as standard_dev,
                        null as min,
                        null as quantile_25,
                        null as quantile_50,
                        null as quantile_75,
                        null as max
                       {% endif %}
                      from prep 
                      group by 1
                    {{"union all " if not loop.last}}
                  {% endfor %} 
            
                )
               select  * from calculation_cte                      
{% endset %}
{{ log(create_table_query, info=True) }}

{% endif %}
{% endmacro %}
