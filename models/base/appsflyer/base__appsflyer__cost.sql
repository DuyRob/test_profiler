{% set sources = get_sources("appsflyer") %}

with source as (
{{ dbt_utils.union_relations(sources) }}
),

renamed as (
    select
        network_app_id,
        media_source,
        agency,
        campaign,
        adset,
        ad_creative,
        keywords,
        campaign_id,
        adset_id,
        ad_creative_id,
        act_date,
        country_code,
        site_id,
        cost,
        installs,
        mapped_app_id
    from source
    -- TODO sync with Amanote team on these rows as currently they contain mostly 'None' data
)

select * from renamed
