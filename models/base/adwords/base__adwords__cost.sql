{% set sources = get_sources("adwords") %}

with source as (
{{ dbt_utils.union_relations(sources) }}
),

renamed as (
    select
        act_date,
        network_app_id, -- this should be ID and not name
        campaign,
        campaign_id,
        location_id, 
        impressions,
        clicks,
        installs,
        raw_cost,
        raw_cost_currency,
        media_source,
        _dbt_source_relation
    from source
)

select
    *,
    {{ dbt_utils.surrogate_key(['campaign_id', '_dbt_source_relation']) }} as id
from renamed
