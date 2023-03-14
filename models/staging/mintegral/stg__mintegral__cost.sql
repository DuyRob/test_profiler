with dim_currency_exchange as (
    select
        currency,
        date,
        max(from_USD) as from_usd
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
        clicks,
        impressions,
        installs,
        preview_link,
        platform,
        raw_cost,
        currency,
        country_code,
        cast(act_date as date) as act_date,
        utc,
        media_source,
        site_id,
        _dbt_source_relation,
        {{ get_agency_from_campaign_name_and_default_rules('media_source', 'campaign', 'network_app_id') }} as agency,
        {{ get_is_cross_promotion('media_source', 'campaign', 'site_id') }} as is_cross_promotion

    from {{ ref('base__mintegral__cost') }}
),

final as (
    select
        id,
        network_app_id,
        campaign,
        campaign_id,
        ad_creative,
        ad_creative_id,
        ad_type,
        clicks,
        impressions,
        installs,
        preview_link,
        platform,
        raw_cost,
        country_code,
        act_date,
        utc,
        media_source,
        site_id,
        _dbt_source_relation,
        raw_cost / from_usd as cost_in_usd
    from mintegral
    left join
        dim_currency_exchange on
            dim_currency_exchange.date = mintegral.act_date and dim_currency_exchange.currency = mintegral.currency
)

select
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
    end as adam_id,
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
    cost_in_usd,
    act_date
from final
