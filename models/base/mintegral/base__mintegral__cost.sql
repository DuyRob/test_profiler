{% set sources = get_sources("mintegral") %}

with source as (
{{ dbt_utils.union_relations(sources) }}
),

renamed as (
    select
        campaign_id,
        network_app_id,
        ad_creative_id,
        clicks,
        impressions,
        installs,
        --uuid, this is not uuid, misleading names
        preview_link,
        campaign,
        geo,
        platform,
        raw_cost,
        country_code,
        ad_creative,
        act_date,
        currency,
        utc,
        media_source,
        --__index_level_0__,
        _dbt_source_relation
    from source
)

select
    *,
    {{ dbt_utils.surrogate_key(['ad_creative_id', 'campaign_id', 'act_date']) }} as id -- TODO UUID not found
from renamed
