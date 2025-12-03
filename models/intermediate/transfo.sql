WITH ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY id
      ORDER BY 
        CASE WHEN existing_contract = 1 THEN 1 ELSE 0 END DESC,  
        count_sales_deals DESC
    ) AS rn
  FROM
    {{ ref('lead_scoring_types_cleaned') }}
)
SELECT
  *
FROM
  ranked
WHERE
  rn = 1
  AND id != 29616229415