with source as (
    select * from {{ source('conversion', 'raw_currency_conversion') }}
),

renamed as (
    select
        cast(from_usd as float64) as from_usd,
        cast(date as date) as exchange_date,
        currency
    from source
)

select
    *,
    {{ dbt_utils.surrogate_key(['date', 'currency']) }} as id

from renamed
