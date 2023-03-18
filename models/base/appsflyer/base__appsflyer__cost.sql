{% set sources = get_sources("appsflyer") %}

with source as (
{{ dbt_utils.union_relations(sources) }}
),

renamed as (
    select
        network_app_id,
        media_source,
        campaign,
        campaign_id,
        adset,
        adset_id,
        ad_creative,
        ad_creative_id,
        keywords,
        act_date,
        site_id,
        cast(cost as float64) as cost, -- already in USD
        cast(installs as int64) as installs,
        _dbt_source_relation,
        array_reverse(
            split(replace(_dbt_source_relation, '`', ''), '_')
        ) [safe_offset(0)] as ingestion_epoch,
        array_reverse(
            split(replace(_dbt_source_relation, '`', ''), '_')
        ) [safe_offset(1)] as ingestion_date,
        {{ dbt_utils.surrogate_key(['network_app_id', 'media_source', 'campaign_id', 'campaign', 'adset_id', 'adset', 'ad_creative_id', 'ad_creative', 
            'keywords', 'act_date', 'site_id', 'country_code']) }} as id,
        case
            when agency = 'None' then null
            when lower(agency) = 'fbmagic' then 'yeahmobi'
            else agency end as agency_from_appsflyer,
        case
            when upper(country_code) = 'UK' then 'GB'
            else upper(country_code)
        end as country_code
    from source
-- TODO sync with Amanote team on these rows as currently they contain mostly 'None' data
),

final as (
    select
        *,
        rank() over (partition by id order by ingestion_epoch desc) as row_number

    from renamed
)

select * from final
where row_number = 1
