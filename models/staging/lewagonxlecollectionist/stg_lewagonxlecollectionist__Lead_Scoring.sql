with 

source as (

    select * from {{ source('lewagonxlecollectionist', 'Lead_Scoring') }}

),

renamed as (

    select
        deal id ,
        contact id,
        deal - client contract email - sha256,
        deal - deal b2b2c,
        deal - deal event,
        deal - static segmentation prospects mkt,
        contact - create date - monthly,
        contact - count lc sales deals,
        contact - lc number of primary,
        contact - m&a import,
        contact - ip country code,
        contact - countryregion,
        contact - city,
        phone number,
        contact - preferred language,
        deal - source,
        deal - app inquiry,
        deal - static latest source,
        deal - deal macro source,
        deal - destination inquiry,
        deal - final booked destination,
        deal - option inquiry,
        deal - requested houses,
        deal - final commission ht,
        deal - partner's commission,
        deal - budget,
        deal - budget per night,
        deal - deal stage macro,
        deal - deal stage,
        deal - lost reason,
        deal - number of sales activities,
        deal - create date - monthly,
        deal - close date - monthly,
        deal - client rental amount,
        deal - time to reach out,
        deal - days to close,
        deal - inquiry check-in date - monthly,
        deal - inquiry check-out date - monthly,
        deal - number of nights,
        deal - booking window inquiry,
        deal - final check-in date - monthly,
        deal - final check-out date - monthly,
        deal - stay duration,
        deal - record source detail 2

    from source

)

select * from renamed