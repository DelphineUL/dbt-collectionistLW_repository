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
ColonneUnique AS City2
FROM one_colonne
WHERE ColonneUnique IN ('Zermatt', 'Saint-Tropez & surroundings', 'Korcula', 'Split and surroundings', 'Verbier', 'Kitzbühel', 'Ibiza', 'Málaga', 'Cap Ferret & the Arcachon bay', 'Sotogrande', 'Barcelona', 'Cadaques', 'Costa Brava', 'Girona', 'South Corsica', 'Alpilles', 'Savoie Tarentaise', 'Menorca', 'Annecy', 'Haute-Savoie, Flaine', 'Upper Savoy, the Aravis', 'haute-Savoie, Portes du Soleil', 'Hautes Alpes', 'Tuscany', 'Saint-Barthelemy', 'Luberon', 'Paros & Antiparos', 'Sankt Anton', 'Basque country & the Landes', 'Puglia', "Côte d'Emeraude", "Côtes-d'Armor", 'Finistère & Morbihan', 'Finistère', 'Château de la Loire', 'Orleans', 'Perche', 'Mallorca', 'North Corsica', 'Paris', 'Milan & Lombardy', 'Bandol & Surroundings', 'Mont-Blanc Massif', 'Côte Fleurie', 'Marbella', 'From Nice to Monaco', 'Grasse country', 'Le Castellet', 'Le Lavandou & surroundings', 'Bordeaux & the vineyards', 'Cannes & surroundings', 'Gard', 'Hérault', 'Le Tarn', 'Mykonos', 'Antibes', 'Etretat', 'The Channel', 'Suisse Normande', 'Terres du Calvados', 'Vexin Normand', 'Nîmes & Uzès', 'Rest of Occitanie', 'Sardinia', 'Carry-le-Rouet', 'Cassis', 'Formentera', 'Athens', 'Athens Riviera', 'Sifnos', 'Amorgos', 'Folegandros', 'Ios', 'Kea', 'Milos', 'Dordogne - Périgord', 'Syros', 'Skiathos', 'Samos', 'Santorini', 'Serifos', 'Naples & Campania', 'Lech', 'Tinos', 'Nisyros', 'Patmos', 'Rhodes', 'Aegina', 'Hydra', 'Spetses', 'Corfu', 'Alonissos', 'Zakynthos', 'Sicily', 'Ithaca', 'Kefalonia', 'Lefkada', 'Paxos', 'Syvota and Surroundings', 'Porto Heli', 'Kalamata and surroundings', 'Brac', 'Ile-de-France', 'Abruzzo', 'Marche', 'Perugia & Umbria', 'Rome & Latium', 'Italian Alps', 'Ile de Ré', 'Province of Genoa', 'Turin & Piedmont', 'Venice & Veneto', 'Pantelleria Island', 'Hvar', 'Marseille', 'Lisbon & surroundings', 'Cascais', 'Lisbon', 'Sintra', 'Gstaad', 'Klosters', 'Rougemont', 'Saint-Moritz', 'Messenia')