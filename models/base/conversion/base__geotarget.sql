with source as (
    select * from {{ source('conversion', 'geotarget') }}
),

renamed as (
    select
        Criteria_ID as id,
        Name as geo_name,
        Country_Code as country_code
    from source
)

select * from renamed
