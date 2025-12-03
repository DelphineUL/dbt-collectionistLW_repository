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

        -- Extraction du pays depuis le numéro de téléphone
        CASE
            -- France (+33 ou 0033)
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)33') THEN 'France'
            -- Royaume-Uni (+44 ou 0044)
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)44') THEN 'Royaume-Uni'
            -- USA / Canada (+1 ou 001)
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)1') THEN 'USA/Canada'
            -- Belgique (+32 ou 0032)
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)32') THEN 'Belgique'
            -- Suisse (+41 ou 0041)
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)41') THEN 'Suisse'
            -- Allemagne (+49 ou 0049)
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)49') THEN 'Allemagne'
            -- Espagne (+34 ou 0034)
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)34') THEN 'Espagne'
            -- Italie (+39 ou 0039)
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)39') THEN 'Italie'
            -- Pays-Bas (+31 ou 0031)
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)31') THEN 'Pays-Bas'
            -- Si le numéro est vide
            WHEN `phone number` IS NULL OR CAST(`phone number` AS STRING) = '' THEN NULL
            ELSE 'Autre' 
        END as phone_country,

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
        
        -- Win/Lost corrigé
        CASE 
            WHEN `deal - deal stage macro` IS NULL THEN NULL
            WHEN LOWER(CAST(`deal - deal stage macro` AS STRING)) LIKE '%won%' THEN 'Win' 
            ELSE 'Lost' 
        END as win_lost,

        `deal - deal stage` as deal_stage,
        `deal - lost reason` as lost_reason,
        `deal - number of sales activities` as nb_sales_activities,
        `deal - create date - monthly` as deal_created_date,
        `deal - close date - monthly` as deal_closed_date,
        
        -- Nettoyage et conversion en FLOAT
        SAFE_CAST(REGEXP_REPLACE(REPLACE(CAST(`deal - client rental amount` AS STRING), ',', '.'), r'[^0-9.-]', '') AS FLOAT64) as client_rental_amount,
        
        -- Création des 6 groupes pour time_to_reach_out
        CASE
            WHEN `deal - time to reach out` IS NULL OR CAST(`deal - time to reach out` AS STRING) = '' THEN NULL
            -- Moins de 2h
            WHEN SAFE_CAST(REGEXP_EXTRACT(TRIM(CAST(`deal - time to reach out` AS STRING)), r'^(\d+)') AS INT64) < 2 THEN '< 2h'
            -- Entre 2h et 6h
            WHEN SAFE_CAST(REGEXP_EXTRACT(TRIM(CAST(`deal - time to reach out` AS STRING)), r'^(\d+)') AS INT64) < 6 THEN '2h - 6h'
            -- Entre 6h et 12h
            WHEN SAFE_CAST(REGEXP_EXTRACT(TRIM(CAST(`deal - time to reach out` AS STRING)), r'^(\d+)') AS INT64) < 12 THEN '6h - 12h'
            -- Entre 12h et 24h
            WHEN SAFE_CAST(REGEXP_EXTRACT(TRIM(CAST(`deal - time to reach out` AS STRING)), r'^(\d+)') AS INT64) < 24 THEN '12h - 24h'
            -- Entre 24h et 168h
            WHEN SAFE_CAST(REGEXP_EXTRACT(TRIM(CAST(`deal - time to reach out` AS STRING)), r'^(\d+)') AS INT64) <= 168 THEN '24h - 168h'
            -- Plus de 168h
            WHEN SAFE_CAST(REGEXP_EXTRACT(TRIM(CAST(`deal - time to reach out` AS STRING)), r'^(\d+)') AS INT64) > 168 THEN '> 168h'
            ELSE NULL 
        END as time_to_reach_out,

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