with ama_app as (
    select
        ironsource_app_id,
        platform,
        max(ama_app_id) as ama_app_id,
        case when bundle_id = '' then null else lower(bundle_id) end as bundle_id,
        case when adam_id = '' then null else lower(adam_id) end as adam_id
    from `amanotes-analytics.pm_dbt.dim_ama_app`
    left join unnest(bundle_ids) as bundle_id
    left join unnest(adam_ids) as adam_id
    left join unnest(ironsource_app_ids) as ironsource_app_id
    group by bundle_id, adam_id, ironsource_app_id, platform
),

cost_ad_network as (
    select * from {{ ref('stg__adwords__cost') }}
    union all
    select * from {{ ref('stg__applovin__cost') }}
    union all
    select * from {{ ref('stg__mintegral__cost') }}
),

mapped_cost_ad_network as (
    select
        cost_ad_network.*,
        ama_app.ama_app_id,
        ama_app.ironsource_app_id
    from cost_ad_network
    left join ama_app on ama_app.platform = cost_ad_network.platform
        and case
            when
                lower(
                    cost_ad_network.platform
                ) = 'android' then ama_app.bundle_id = cost_ad_network.bundle_id
            when
                lower(
                    cost_ad_network.platform
                ) = 'ios' then ama_app.adam_id = cost_ad_network.adam_id
        end
)

select *
from mapped_cost_ad_network