with 

source as (

    select * from {{ source('lewagonxlecollectionist', 'Indicatif') }}

),

renamed as (

    select
        string_field_0,
        string_field_1

    from source

)

select * from renamed