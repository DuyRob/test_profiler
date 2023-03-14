with adwords as (
    select
        act_date,
        network_app_id,
        campaign,
        cast(campaign_id as string) as campaign_id,
        location_id,
        impressions,
        clicks,
        installs,
        -- These are referenced from the media_source_pre in adwords_cost
        raw_cost_currency,
        media_source,
        site_id,
        id,
        cast(raw_cost as float64) / 1000000 as raw_cost,
        {{ get_agency_from_campaign_name_and_default_rules('media_source', 'campaign', 'network_app_id') }} as agency,
        {{ get_is_cross_promotion('media_source', 'campaign', '') }} as is_cross_promotion -- site_id is required as 3rd argument, but new raw sources do not have this data. Will need check again with project team.

    from {{ ref('base__adwords__cost') }}
),

dim_currency_exchange as (
    select
        currency,
        date,
        max(from_USD) as from_usd
    from `amanotes-analytics.performance_monitoring.dim_currency_exchange`
    group by currency, date
),

final as (select
    adwords.*,
    dim_geotarget.Country_Code as country_code,
    cast(null as string) as ad_type,
    raw_cost / from_usd as cost_in_usd
    from adwords
    left join
        `amanotes-analytics.performance_monitoring.dim_geotarget` as dim_geotarget on
            dim_geotarget.Criteria_ID = adwords.location_id
    left join
        dim_currency_exchange on
            dim_currency_exchange.date = adwords.act_date and dim_currency_exchange.currency = adwords.raw_cost_currency
)

select
    '__NA__' as bundle_id,
    '__NA__' as adam_id,
    cast(null as string) as platform,
    network_app_id,
    country_code,
    media_source,
    campaign,
    campaign_id,
    cast(null as string) as ad_creative,
    cast(null as string) as ad_creative_id,
    ad_type,
    site_id,
    impressions,
    clicks,
    installs,
    cost_in_usd,
    act_date
from final
