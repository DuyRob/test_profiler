{% set sources = get_sources("mintegral") %}

with source as (
{{ dbt_utils.union_relations(sources) }}
),

renamed as (
    select
        network_app_id,
        campaign,
        campaign_id,
        ad_creative,
        ad_creative_id,
        cast(null as string) as ad_type,
        clicks,
        impressions,
        installs,
        --uuid, this is not uuid, misleading names
        preview_link,
        platform,
        raw_cost,
        country_code,
        cast(act_date as date) as act_date,
        currency,
        utc,
        media_source,
        site_id,
        _dbt_source_relation,
        {{ dbt_utils.surrogate_key(['site_id', 'ad_creative_id',
            'campaign_id', 'act_date', 'country_code']) }} as id,
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
