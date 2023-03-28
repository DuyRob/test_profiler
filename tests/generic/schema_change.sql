{% test schema_change( model ) %}


with previous_updated_date as(
    select distinct profiled_at, 
    dense_rank() OVER(ORDER BY profiled_at DESC) as update_rank
    FROM {{ model }}
    qualify update_rank = 2

)

select *
from (
    select
       
     column_name ||'-'|| data_type

    from {{ model }}
    where profiled_at = current_date()

    and column_name || data_type not in 
    ( select  column_name || data_type from {{model}}  where profiled_at = (select profiled_at from previous_updated_date))

    UNION ALL 

    select
       
     column_name ||'-'|| data_type

    from {{ model }}
    where profiled_at = (select profiled_at from previous_updated_date)

    and column_name || data_type  not in 
    ( select column_name || data_type  from {{model}}  where profiled_at = current_date())

) validation_errors

{% endtest %}
