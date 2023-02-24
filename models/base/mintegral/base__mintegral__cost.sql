{% set sources = get_sources("mintegral") %}

with source as (
{{ dbt_utils.union_relations(sources) }}
),

renamed as (
    select
        clicks,
        impressions,
        installs,
        campaign_id,
        uuid,
        preview_link,
        campaign,
        geo,
        platform,
        network_app_id,
        raw_cost,
        country_code,
        ad_creative,
        act_date,
        currency,
        utc,
        ad_creative_id,
        media_source,
        __index_level_0__
    from source
)

select
    *,
    {{ dbt_utils.surrogate_key(['ad_creative_id', 'campaign_id']) }} as id -- TODO UUID not found
from renamed
