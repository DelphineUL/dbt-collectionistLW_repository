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
ColonneUnique AS City3
FROM one_colonne
WHERE ColonneUnique IN ('Courchevel', 'Samoëns', 'La Clusaz', 'Le Grand Bornand', "Les Carroz d'Arâches", 'Chatel', 'Les Gets', 'Morzine', 'Montgenèvre', 'Serre Chevalier', "Val d'Isère", 'Méribel', "Saint Tropez Peninsula", 'Cap Ferret', 'Tignes', "Arcachon bay", 'Ramatuelle', 'Saint-Remy-de-Provence & surroundings', 'Grimaud', 'La Plagne', 'Les Arcs', 'Biarritz & Basque country', 'Saint-Martin-de-Belleville', 'Sainte-Foy-Tarentaise', 'Val Thorens', 'Calvi', 'Cap Corse', 'Côte Verte', 'Ile Rousse', 'Saint-Florent', 'Ajaccio', 'Bonifacio', 'Coti Chiavari', 'Figari', 'Pianottoli-Caldarello', 'Propriano', 'Region of Porto-Vecchio', 'Sartène', 'Spérone', 'Lake Como', 'Maussane-les-Alpilles and surroundings', 'Gassin', 'Gordes & surroundings', 'La Croix-Valmer', 'Deauville - Trouville', 'Sainte-Maxime', 'Eygalieres & surroundings', "Cap d'Ail", 'Eze', 'Monaco', 'Nice', 'Roquebrune-Cap-Martin', 'Saint-Paul-de-Vence', 'The Landes', 'Rayol-Canadel-sur-Mer', 'Grasse', 'La Colle-sur-Loup', 'Surroundings of Bordeaux','Megève & surroundings', 'Cannes', 'Draguignan', 'Aix-en-Provence & surroundings', 'Chamonix & surroundings', 'Bonnieux and surroundings', 'Béziers & surroundings', 'Lourmarin & surroundings', 'Mougins', 'Hyères', "Cap d'Antibes", 'Arles & the Camargue', 'Bandol', 'Sanary', 'Bormes-les-Mimosas', "L'isle-sur-la-Sorgue", 'Menerbes and surroundings', 'Oppede', 'Antibes and surroundings', 'Juan-les-Pins', 'Villefranche-sur-Mer' 'Cavalaire-sur-Mer', 'Avignon', 'Antiparos', 'Paros', 'Perugia', 'Lazio', 'Rome', 'Florence', 'Province of Arezzo', 'Province of Grosseto', 'Province of Livorno', 'Province of Lucca', 'Province of Pisa', 'Province of Siena', 'Dolomites & surroundings', 'Lake Garda', 'Milan', 'Sestriere', 'Turin', 'Venice', 'Salento', 'Ostuni', 'Province of Bari', 'Amalfi Coast', 'Naples', 'Cilento National Park')