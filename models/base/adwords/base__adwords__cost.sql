{% set sources = get_sources("adwords") %}

with source as (
{{ dbt_utils.union_relations(sources) }}
),

renamed as (
    select
        cast(act_date as date) as act_date,
        network_app_id, -- this should be ID and not name
        campaign,
        cast(campaign_id as string) as campaign_id,
        location_id,
        impressions,
        clicks,
        installs,
        raw_cost_currency,
        media_source,
        cast(null as string) as site_id,
        _dbt_source_relation,
        array_reverse(
            split(replace(_dbt_source_relation, '`', ''), '_')
        ) [safe_offset(0)] as ingestion_epoch,
        array_reverse(
            split(replace(_dbt_source_relation, '`', ''), '_')
        ) [safe_offset(1)] as ingestion_date,
        {{ dbt_utils.surrogate_key(['location_id', 'campaign_id', 'act_date']) }} as id,
        cast(raw_cost as float64) / 1000000 as raw_cost
    from source
),

final as (
    select
        * except (ingestion_date),
        parse_date('%Y%m%d', ingestion_date) as ingestion_date,
        rank() over (partition by id order by ingestion_epoch desc) as row_number

    from renamed
)

select * from final
where row_number = 1
