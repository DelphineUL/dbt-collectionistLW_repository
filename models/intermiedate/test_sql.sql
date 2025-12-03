with source as (
    select * from {{ source('lewagonxlecollectionist', 'Lead_Scoring') }}
),
renamed as (
    select
        `deal id ` AS  id,
        `contact id` AS contact_id,
        `deal - client contract email - sha256` AS contract_email,
        `deal - deal b2b2c` AS b2b2c,
        `deal - deal event` AS event,
        `deal - static segmentation prospects mkt` AS segmentation_mkt,
        `contact - create date - monthly` AS contact_created_date,
        `contact - count lc sales deals` AS count_sales_deals,
        `contact - lc number of primary` AS existing_contract,
        `contact - m&a import` AS m_a,
        `contact - ip country code` as ip_country_code,
        `contact - countryregion` as country_region,
        `contact - city` as city,
        `phone number` as phone_number,
        `deal - source` as source,
        `deal - app inquiry` as app,
        `deal - static latest source` as source_mkt,
        `deal - deal macro source` as macro_source,
        `deal - destination inquiry` as destination_inquiry,
        `deal - final booked destination` as final_booked_destination,
        `deal - option inquiry` as option,
        `deal - requested houses` as requested_houses,
        
        -- Nettoyage et conversion en FLOAT (Commission)
        SAFE_CAST(REGEXP_REPLACE(REPLACE(CAST(`deal - final commission ht` AS STRING), ',', '.'), r'[^0-9.-]', '') AS FLOAT64) as commission,
        
        -- Nettoyage et conversion en FLOAT (Partner Commission)
        SAFE_CAST(REGEXP_REPLACE(REPLACE(CAST(`deal - partner's commission` AS STRING), ',', '.'), r'[^0-9.-]', '') AS FLOAT64) AS partner_commission,
        
        -- Nettoyage et conversion en FLOAT (Budget)
        SAFE_CAST(REGEXP_REPLACE(REPLACE(CAST(`deal - budget` AS STRING), ',', '.'), r'[^0-9.-]', '') AS FLOAT64) as budget,
        
        -- Nettoyage et conversion en FLOAT (Budget par nuit)
        SAFE_CAST(REGEXP_REPLACE(REPLACE(CAST(`deal - budget per night` AS STRING), ',', '.'), r'[^0-9.-]', '') AS FLOAT64) as budget_per_night,
        
        -- Conversion de win_lost en Texte ('Win' ou 'Lost')
        CASE 
            WHEN LOWER(CAST(`deal - deal stage macro` AS STRING)) LIKE '%won%' THEN 'Win' 
            ELSE 'Lost' 
        END as win_lost,

        `deal - deal stage` as deal_stage,
        `deal - lost reason` as lost_reason,
        `deal - number of sales activities` as nb_sales_activities,
        `deal - create date - monthly` as deal_created_date,
        `deal - close date - monthly` as deal_closed_date,
        
        -- Nettoyage et conversion en FLOAT (Montant location client)
        SAFE_CAST(REGEXP_REPLACE(REPLACE(CAST(`deal - client rental amount` AS STRING), ',', '.'), r'[^0-9.-]', '') AS FLOAT64) as client_rental_amount,
        
        `deal - time to reach out` as time_to_reach_out,
        `deal - days to close` as days_to_close,
        `deal - inquiry check-in date - monthly` as inquiry_check_in_date,
        `deal - inquiry check-out date - monthly` as inquiry_check_out_date,
        `deal - number of nights` as nb_nights,
        
        -- Conversion sécurisée (Heures -> Jours entiers)
        CAST(SAFE_CAST(SPLIT(CAST(`deal - booking window inquiry` AS STRING), ':')[SAFE_OFFSET(0)] AS INT64) / 24 AS INT64) as booking_window_inquiry,
        
        `deal - final check-in date - monthly` as final_check_in_date,
        `deal - final check-out date - monthly` as final_check_out_date,
        
        -- Conversion sécurisée (Heures -> Jours entiers)
        CAST(SAFE_CAST(SPLIT(CAST(`deal - stay duration` AS STRING), ':')[SAFE_OFFSET(0)] AS INT64) / 24 AS INT64) as stay_duration

    from source
)
select * from renamed


