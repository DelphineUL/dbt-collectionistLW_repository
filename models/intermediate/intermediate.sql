Select
SAFE_CAST(commission AS FLOAT64) AS commission_clean
From {{ ref('stg_lewagonxlecollectionist__Lead_Scoring') }}
WHERE commission_clean IS NOT NULL