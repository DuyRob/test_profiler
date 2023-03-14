{% set sources = get_sources("mintegral") %}

with source as (
{{ dbt_utils.union_relations(sources) }}
),

renamed as (
    select
        network_app_id,
        campaign,
        campaign_id,
        ad_creative,
        ad_creative_id,
        cast(null as string) as ad_type,
        clicks,
        impressions,
        installs,
        --uuid, this is not uuid, misleading names
        preview_link,
        platform,
        raw_cost,
        country_code,
        act_date,
        currency,
        utc,
        media_source,
        site_id,
        _dbt_source_relation
    from source
)

select
    *,
    {{ dbt_utils.surrogate_key(['ad_creative_id'
        , 'campaign_id', 'act_date', 'country_code']) }} as id -- TODO UUID not found
from renamed
