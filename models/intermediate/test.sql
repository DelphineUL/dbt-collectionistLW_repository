select
--CAST(budget_per_night  AS FLOAT64) AS budget_per_night,
CAST(budget  AS FLOAT64) AS budget
--CAST(client_rental_amount AS FLOAT64) as client_rental
from {{ ref('stg_lewagonxlecollectionist__Lead_Scoring') }}