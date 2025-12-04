WITH split_desti AS(SELECT
    destination_inquiry,
    -- Le 1er élément (index 0) de l'array
    SPLIT(destination_inquiry, '; ')[SAFE_OFFSET(0)] AS A,
    -- Le 2ème élément (index 1) de l'array
    SPLIT(destination_inquiry, '; ')[SAFE_OFFSET(1)] AS B,
    -- Le 3ème élément (index 2) de l'array
    SPLIT(destination_inquiry, '; ')[SAFE_OFFSET(2)] AS C,
        -- Le 4ème élément (index 3) de l'array
    SPLIT(destination_inquiry, '; ')[SAFE_OFFSET(3)] AS D,
         -- Le 5ème élément (index 4) de l'array
    SPLIT(destination_inquiry, '; ')[SAFE_OFFSET(4)] AS E,
             -- Le 5ème élément (index 5) de l'array
    SPLIT(destination_inquiry, '; ')[SAFE_OFFSET(5)] AS F,
                 -- Le 5ème élément (index 6) de l'array
    SPLIT(destination_inquiry, '; ')[SAFE_OFFSET(6)] AS G,
                     -- Le 5ème élément (index 7) de l'array
    SPLIT(destination_inquiry, '; ')[SAFE_OFFSET(7)] AS H,
                         -- Le 5ème élément (index 8) de l'array
    SPLIT(destination_inquiry, '; ')[SAFE_OFFSET(8)] AS I,
                             -- Le 5ème élément (index 9) de l'array
    SPLIT(destination_inquiry, '; ')[SAFE_OFFSET(9)] AS J
FROM {{ ref('lead_scoring_types_cleaned') }}
WHERE destination_inquiry IS NOT NULL)
,

one_colonne AS (
SELECT A AS ColonneUnique FROM split_desti
UNION ALL
SELECT B FROM split_desti
UNION ALL
SELECT C FROM split_desti
UNION ALL
SELECT D FROM split_desti
UNION ALL
SELECT E FROM split_desti
UNION ALL
SELECT F FROM split_desti
UNION ALL
SELECT G FROM split_desti
UNION ALL
SELECT H FROM split_desti
UNION ALL
SELECT I FROM split_desti
UNION ALL
SELECT J FROM split_desti
)

SELECT
ColonneUnique AS City
FROM one_colonne
WHERE ColonneUnique IN ('Dalmatia', 'Dubrovnik & surroundings', 'Pasman Island', 'Istria', 'Split', 'Swiss Alps', 'French Riviera', 'Austrian Alps', 'Balearic islands', 'Crete', 'Andalusia', 'Southwest', 'Castilla La Mancha', 'Castile and León', 'Catalonia', 'Barcelona & the Catalonia', 'Galicia', 'Corsica', 'Provence', 'French Alps', 'Central Italy', 'Lesser Antilles', 'Cyclades', 'Southern Italy', 'Auvergne-Rhône-Alpes', 'Brittany', 'Centre - Val de Loire', 'Paris & Ile-de-France', 'Northern Italy', 'Normandy' 'Dijon & Burgundy', 'Languedoc-Roussillon', 'Loire-Atlantique', 'Loiret', 'Occitanie', 'Athens & surroundings', 'Western Greece', 'Sporades islands', 'Dodecanese islands', 'Saronic islands', 'The Ionian islands', 'Peloponnese', 'Amphilochia Province', 'Azores', 'Alentejo', 'Algarve', 'Comporta & Melides', 'Madeira', 'Porto & surroundings', 'Lisbon Region', 'Douro valley', 'Marrakech')