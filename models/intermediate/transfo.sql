WITH ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY id
      ORDER BY 
        CASE WHEN existing_contract = 1 THEN 1 ELSE 0 END DESC,  -- prefer existing_contract=1
        count_sales_deals DESC                                      -- then max count_sales_deals
    ) AS rn
  FROM
    models/intermediate/lead_scoring_types_cleaned.sql )
SELECT
*
FROM
  ranked
WHERE
  rn = 1
AND id != 29616229415
