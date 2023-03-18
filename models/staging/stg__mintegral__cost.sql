with dim_currency_exchange as (
    select
        currency,
        date,
        max(from_usd) as from_usd
    from `amanotes-analytics.performance_monitoring.dim_currency_exchange`
    group by currency, date
),

mintegral as (
    select
        id,
        network_app_id,
        campaign,
        campaign_id,
        ad_creative,
        ad_creative_id,
        ad_type,
        preview_link,
        platform,
        currency,
        country_code,
        act_date,
        utc,
        media_source,
        site_id,
        {{ get_agency_from_campaign_name_and_default_rules('media_source', 'campaign', 'network_app_id') }} as agency,
        {{ get_is_cross_promotion('media_source', 'campaign', 'site_id') }} as is_cross_promotion,
        sum(clicks) as clicks,
        sum(impressions) as impressions,
        sum(installs) as installs,
        sum(raw_cost) as raw_cost

    from {{ ref('base__mintegral__cost') }}
    {{ dbt_utils.group_by(16) }}
),

final as (
    select
        mintegral.id,
        mintegral.network_app_id,
        mintegral.campaign,
        mintegral.campaign_id,
        mintegral.ad_creative,
        mintegral.ad_creative_id,
        mintegral.ad_type,
        mintegral.clicks,
        mintegral.impressions,
        mintegral.installs,
        mintegral.preview_link,
        mintegral.platform,
        mintegral.raw_cost,
        mintegral.country_code,
        mintegral.act_date,
        mintegral.utc,
        mintegral.media_source,
        mintegral.site_id,
        mintegral.raw_cost / dim_currency_exchange.from_usd as cost_in_usd
    from mintegral
    left join
        dim_currency_exchange on
            dim_currency_exchange.date = mintegral.act_date and dim_currency_exchange.currency = mintegral.currency
)

select
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
    act_date,
    id,
    impressions,
    clicks,
    installs,
    cost_in_usd,
    case
        when lower(platform) = 'android' and starts_with(network_app_id, 'com.') then network_app_id
    end as bundle_id,
    case
        when
            lower(
                platform
            ) = 'ios' and regexp_contains(
                network_app_id, r'^id[0-9]'
            ) then substr(network_app_id, 3, length(network_app_id))
    end as adam_id
from final
