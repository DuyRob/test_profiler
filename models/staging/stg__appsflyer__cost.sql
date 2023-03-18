with appsflyer as (
    select
        network_app_id,
        media_source,
        agency_from_appsflyer,
        campaign,
        campaign_id,
        adset,
        adset_id,
        ad_creative,
        ad_creative_id,
        keywords,
        act_date,
        country_code,
        site_id,
        cost,
        installs,
        coalesce({{ get_is_cross_promotion('media_source', 'campaign', 'site_id') }}, false) as is_cross_promotions,
        {{ get_agency_from_campaign_name_and_default_rules('media_source', 'campaign', 'network_app_id') }} as agency_from_campaign_name
    from {{ ref('base__appsflyer__cost') }}
),

dim_ama_app as (select
        appsflyer_app_id,
        ama_app_id
    from
        `amanotes-analytics.pm_dbt.dim_ama_app` as dim_ama_app_ext,
        unnest(appsflyer_app_ids) as appsflyer_app_id
    where
        appsflyer_app_id != ''
        and exists (
            select 1
            from
                `amanotes-analytics.pm_dbt.dim_ama_app`,
                unnest(appsflyer_app_ids) as appsflyer_app_id_max
            group by appsflyer_app_id_max
            having
                max(
                    created_at
                ) = dim_ama_app_ext.created_at and appsflyer_app_id = appsflyer_app_id_max
        )
),

appsflyer_mapped as (select
        appsflyer.*,
        cast(dim_ama_app.ama_app_id as string) as ama_app_id
    from appsflyer
    left join dim_ama_app
        on appsflyer.network_app_id = dim_ama_app.appsflyer_app_id
),

appsflyer_agg as (
    select
        act_date,
        ama_app_id,
        network_app_id,
        country_code,
        case
            when substr(cast(ama_app_id as string), -1) = 'a' then 'android'
            when substr(cast(ama_app_id as string), -1) = 'i' then 'ios'
            when substr(cast(ama_app_id as string), -1) = 'f' then 'facebook'
        end as platform,
        media_source,
        agency_from_appsflyer,
        agency_from_campaign_name,
        campaign,
        campaign_id,
        adset,
        adset_id,
        ad_creative,
        ad_creative_id,
        cast(null as string) as ad_type,
        keywords,
        cast(null as string) as kw_match_type,
        site_id,
        is_cross_promotions,
        sum(cast(null as int64)) as impressions,
        sum(cast(null as int64)) as clicks,
        sum(installs) as installs,
        sum(cost) as cost
    from appsflyer_mapped
    {{ dbt_utils.group_by(19) }}
)

select * from appsflyer_agg
