with applovin as (
    select
        campaign,
        campaign_id,
        ad_creative,
        ad_creative_id,
        act_date,
        site_id,
        impressions,
        clicks,
        installs,
        ad_type,
        country_code,
        raw_cost,
        platform,
        network_app_id,
        media_source,
        {{ get_agency_from_campaign_name_and_default_rules('media_source', 'campaign', 'network_app_id') }} as agency,
        {{ get_is_cross_promotion('media_source', 'campaign', '') }} as is_cross_promotion -- site_id is required as 3rd argument, but new raw sources do not have this data. Will need check again with project team.

    from {{ ref('base__applovin__cost') }}
)

select
    cast(network_app_id as string) as bundle_id,
    '__NA__' as adam_id,
    platform,
    network_app_id,
    country_code,
    media_source,
    campaign,
    campaign_id,
    ad_creative,
    ad_creative_id,
    ad_type,
    site_id,
    impressions,
    clicks,
    installs,
    raw_cost as cost_in_usd,
    act_date
from applovin
