SELECT
    destination_inquiry,
    -- Le 1er élément (index 0) de l'array
    SPLIT(destination_inquiry, ';')[SAFE_OFFSET(0)] AS A,
    -- Le 2ème élément (index 1) de l'array
    SPLIT(destination_inquiry, ';')[SAFE_OFFSET(1)] AS B,
    -- Le 3ème élément (index 2) de l'array
    SPLIT(destination_inquiry, ';')[SAFE_OFFSET(2)] AS C,
        -- Le 4ème élément (index 3) de l'array
    SPLIT(destination_inquiry, ';')[SAFE_OFFSET(3)] AS D,
         -- Le 5ème élément (index 4) de l'array
    SPLIT(destination_inquiry, ';')[SAFE_OFFSET(4)] AS E,
             -- Le 5ème élément (index 5) de l'array
    SPLIT(destination_inquiry, ';')[SAFE_OFFSET(5)] AS F,
                 -- Le 5ème élément (index 6) de l'array
    SPLIT(destination_inquiry, ';')[SAFE_OFFSET(6)] AS G,
                     -- Le 5ème élément (index 7) de l'array
    SPLIT(destination_inquiry, ';')[SAFE_OFFSET(7)] AS H,
                         -- Le 5ème élément (index 8) de l'array
    SPLIT(destination_inquiry, ';')[SAFE_OFFSET(8)] AS I,
                             -- Le 5ème élément (index 9) de l'array
    SPLIT(destination_inquiry, ';')[SAFE_OFFSET(9)] AS J
FROM {{ ref('lead_scoring_types_cleaned') }}
WHERE destination_inquiry IS NOT NULL