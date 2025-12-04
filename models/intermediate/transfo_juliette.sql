SELECT
    destination_inquiry,
    LENGTH(destination_inquiry) - LENGTH(REPLACE(destination_inquiry, ';', '')) AS nombre_de_points_virgules
FROM
  {{ ref('lead_scoring_types_cleaned') }}
  WHERE destination_inquiry IS NOT NULL 
  ORDER BY nombre_de_points_virgules DESC