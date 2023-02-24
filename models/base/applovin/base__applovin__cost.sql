{% set sources = get_sources("applovin") %}

with source as (
{{ dbt_utils.union_relations(sources) }}
),

renamed as (
    select
        campaign_id,
        ad_creative_id,
        act_date,
        impressions,
        clicks,
        installs,
        ad_creative,
        ad_type,
        country_code,
        campaign,
        raw_cost,
        platform,
        network_app_id,
        media_source,
        __index_level_0__
    from source
)

select
    *,
    {{ dbt_utils.surrogate_key(['__index_level_0__', 'campaign_id']) }} as id
from renamed
