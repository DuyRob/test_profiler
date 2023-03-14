{% set sources = get_sources("appsflyer") %}

with source as (
{{ dbt_utils.union_relations(sources) }}
),

renamed as (
    select
        network_app_id,
        media_source,
        agency,
        campaign,
        campaign_id,
        adset,
        adset_id,
        ad_creative,
        ad_creative_id,
        keywords,
        act_date,
        site_id,
        cast(cost as float64) as cost,
        cast(installs as int64) as installs, -- already in USD
        case
            when upper(country_code) = 'UK' then 'GB'
            else upper(country_code)
        end as country_code
    from source
-- TODO sync with Amanote team on these rows as currently they contain mostly 'None' data
)

select
    *,
    {{ dbt_utils.surrogate_key(['campaign_id', 'site_id']) }} as id
from renamed
