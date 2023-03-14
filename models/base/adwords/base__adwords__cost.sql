{% set sources = get_sources("adwords") %}

with source as (
{{ dbt_utils.union_relations(sources) }}
),

renamed as (
    select
        cast(act_date as date) as act_date,
        network_app_id, -- this should be ID and not name
        campaign,

        cast(campaign_id as string) as campaign_id,
        location_id,
        impressions,
        clicks,
        installs,
        raw_cost,
        raw_cost_currency,
        media_source,
        cast(null as string) as site_id,
        __index_level_0__
    from source
)

select
    *,
    {{ dbt_utils.surrogate_key(['location_id', 'campaign_id', 'act_date']) }} as id

from renamed
