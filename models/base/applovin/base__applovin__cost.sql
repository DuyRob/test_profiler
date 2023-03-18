{% set sources = get_sources("applovin") %}

with source as (
{{ dbt_utils.union_relations(sources) }}
),

renamed as (
    select
        campaign,
        campaign_id,
        ad_creative,
        ad_creative_id,
        cast(act_date as date) as act_date,
        cast(null as string) as site_id,
        cast(impressions as int64) as impressions,
        cast(clicks as int64) as clicks,
        cast(installs as int64) as installs,
        ad_type,
        country_code,
        cast(raw_cost as float64) as raw_cost,
        platform,
        network_app_id,
        media_source,
        _dbt_source_relation,
        {{ dbt_utils.surrogate_key(['act_date', 'country_code',
            'campaign_id', 'ad_creative_id', 'ad_type']) }} as id,
        array_reverse(
            split(replace(_dbt_source_relation, '`', ''), '_')
        ) [safe_offset(0)] as ingestion_epoch,
        array_reverse(
            split(replace(_dbt_source_relation, '`', ''), '_')
        ) [safe_offset(1)] as ingestion_date
    from source
),

final as (
    select
        *,
        rank() over (partition by id order by ingestion_epoch desc) as row_number
    from renamed
)

select * from final
where row_number = 1
