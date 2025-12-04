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

        -- Extraction du pays depuis le numéro de téléphone (Liste étendue ~170 pays)
        CASE
            -- === ZONE 3 & 4: Europe ===
            -- France (+33, 0033 OU format national 01-09)
            -- Ajout de |^0[1-9] pour capturer 06, 07, 01, etc.
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)33|^0[1-9]') THEN 'France'
            
            -- === ZONE 1: Amérique du Nord (NANP) ===
            -- Attention: USA/Canada partagent +1. On teste d'abord les îles spécifiques si besoin, sinon on groupe.
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)1') THEN 'USA/Canada'

            -- === ZONE 2: Afrique ===
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)20') THEN 'Égypte'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)212') THEN 'Maroc'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)213') THEN 'Algérie'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)216') THEN 'Tunisie'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)218') THEN 'Libye'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)220') THEN 'Gambie'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)221') THEN 'Sénégal'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)222') THEN 'Mauritanie'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)223') THEN 'Mali'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)224') THEN 'Guinée'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)225') THEN 'Côte d’Ivoire'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)226') THEN 'Burkina Faso'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)227') THEN 'Niger'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)228') THEN 'Togo'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)229') THEN 'Bénin'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)230') THEN 'Maurice'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)231') THEN 'Liberia'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)232') THEN 'Sierra Leone'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)233') THEN 'Ghana'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)234') THEN 'Nigeria'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)235') THEN 'Tchad'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)236') THEN 'Rép. Centrafricaine'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)237') THEN 'Cameroun'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)238') THEN 'Cap-Vert'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)239') THEN 'Sao Tomé-et-Principe'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)240') THEN 'Guinée équatoriale'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)241') THEN 'Gabon'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)242') THEN 'Congo-Brazzaville'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)243') THEN 'RDC (Congo-Kinshasa)'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)244') THEN 'Angola'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)245') THEN 'Guinée-Bissau'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)246') THEN 'Diego Garcia'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)248') THEN 'Seychelles'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)249') THEN 'Soudan'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)250') THEN 'Rwanda'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)251') THEN 'Éthiopie'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)252') THEN 'Somalie'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)253') THEN 'Djibouti'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)254') THEN 'Kenya'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)255') THEN 'Tanzanie'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)256') THEN 'Ouganda'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)257') THEN 'Burundi'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)258') THEN 'Mozambique'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)260') THEN 'Zambie'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)261') THEN 'Madagascar'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)262') THEN 'Réunion/Mayotte'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)263') THEN 'Zimbabwe'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)264') THEN 'Namibie'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)265') THEN 'Malawi'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)266') THEN 'Lesotho'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)267') THEN 'Botswana'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)268') THEN 'Eswatini'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)269') THEN 'Comores'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)27') THEN 'Afrique du Sud'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)290') THEN 'Sainte-Hélène'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)291') THEN 'Érythrée'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)297') THEN 'Aruba'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)298') THEN 'Îles Féroé'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)299') THEN 'Groenland'

            -- === SUITE EUROPE ===
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)30') THEN 'Grèce'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)31') THEN 'Pays-Bas'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)32') THEN 'Belgique'
            -- France déplacé en haut pour priorité
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)34') THEN 'Espagne'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)350') THEN 'Gibraltar'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)351') THEN 'Portugal'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)352') THEN 'Luxembourg'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)353') THEN 'Irlande'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)354') THEN 'Islande'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)355') THEN 'Albanie'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)356') THEN 'Malte'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)357') THEN 'Chypre'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)358') THEN 'Finlande'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)359') THEN 'Bulgarie'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)36') THEN 'Hongrie'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)370') THEN 'Lituanie'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)371') THEN 'Lettonie'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)372') THEN 'Estonie'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)373') THEN 'Moldavie'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)374') THEN 'Arménie'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)375') THEN 'Biélorussie'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)376') THEN 'Andorre'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)377') THEN 'Monaco'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)378') THEN 'Saint-Marin'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)380') THEN 'Ukraine'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)381') THEN 'Serbie'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)382') THEN 'Monténégro'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)385') THEN 'Croatie'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)386') THEN 'Slovénie'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)387') THEN 'Bosnie-Herzégovine'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)389') THEN 'Macédoine du Nord'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)39') THEN 'Italie'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)40') THEN 'Roumanie'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)41') THEN 'Suisse'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)420') THEN 'République tchèque'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)421') THEN 'Slovaquie'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)423') THEN 'Liechtenstein'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)43') THEN 'Autriche'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)44') THEN 'Royaume-Uni'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)45') THEN 'Danemark'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)46') THEN 'Suède'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)47') THEN 'Norvège'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)48') THEN 'Pologne'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)49') THEN 'Allemagne'

            -- === ZONE 5: Amérique Latine ===
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)500') THEN 'Malouines'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)501') THEN 'Belize'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)502') THEN 'Guatemala'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)503') THEN 'Salvador'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)504') THEN 'Honduras'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)505') THEN 'Nicaragua'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)506') THEN 'Costa Rica'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)507') THEN 'Panama'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)508') THEN 'Saint-Pierre-et-Miquelon'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)509') THEN 'Haïti'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)51') THEN 'Pérou'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)52') THEN 'Mexique'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)53') THEN 'Cuba'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)54') THEN 'Argentine'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)55') THEN 'Brésil'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)56') THEN 'Chili'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)57') THEN 'Colombie'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)58') THEN 'Venezuela'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)590') THEN 'Guadeloupe'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)591') THEN 'Bolivie'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)592') THEN 'Guyana'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)593') THEN 'Équateur'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)594') THEN 'Guyane française'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)595') THEN 'Paraguay'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)596') THEN 'Martinique'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)597') THEN 'Suriname'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)598') THEN 'Uruguay'

            -- === ZONE 6: Océanie & Asie du Sud-Est ===
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)60') THEN 'Malaisie'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)61') THEN 'Australie'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)62') THEN 'Indonésie'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)63') THEN 'Philippines'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)64') THEN 'Nouvelle-Zélande'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)65') THEN 'Singapour'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)66') THEN 'Thaïlande'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)670') THEN 'Timor oriental'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)673') THEN 'Brunei'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)674') THEN 'Nauru'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)675') THEN 'Papouasie-Nouvelle-Guinée'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)676') THEN 'Tonga'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)677') THEN 'Îles Salomon'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)678') THEN 'Vanuatu'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)679') THEN 'Fidji'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)680') THEN 'Palaos'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)681') THEN 'Wallis-et-Futuna'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)682') THEN 'Îles Cook'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)685') THEN 'Samoa'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)686') THEN 'Kiribati'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)687') THEN 'Nouvelle-Calédonie'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)688') THEN 'Tuvalu'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)689') THEN 'Polynésie française'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)690') THEN 'Tokelau'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)691') THEN 'Micronésie'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)692') THEN 'Îles Marshall'

            -- === ZONE 7: Russie & Kazakhstan ===
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)7') THEN 'Russie/Kazakhstan'

            -- === ZONE 8: Asie de l Est ===
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)81') THEN 'Japon'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)82') THEN 'Corée du Sud'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)84') THEN 'Vietnam'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)850') THEN 'Corée du Nord'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)852') THEN 'Hong Kong'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)853') THEN 'Macao'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)855') THEN 'Cambodge'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)856') THEN 'Laos'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)86') THEN 'Chine'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)880') THEN 'Bangladesh'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)886') THEN 'Taïwan'

            -- === ZONE 9: Asie de l Ouest & du Sud ===
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)90') THEN 'Turquie'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)91') THEN 'Inde'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)92') THEN 'Pakistan'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)93') THEN 'Afghanistan'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)94') THEN 'Sri Lanka'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)95') THEN 'Birmanie (Myanmar)'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)960') THEN 'Maldives'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)961') THEN 'Liban'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)962') THEN 'Jordanie'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)963') THEN 'Syrie'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)964') THEN 'Irak'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)965') THEN 'Koweït'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)966') THEN 'Arabie saoudite'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)967') THEN 'Yémen'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)968') THEN 'Oman'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)970') THEN 'Palestine'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)971') THEN 'Émirats arabes unis'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)972') THEN 'Israël'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)973') THEN 'Bahreïn'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)974') THEN 'Qatar'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)975') THEN 'Bhoutan'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)976') THEN 'Mongolie'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)977') THEN 'Népal'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)98') THEN 'Iran'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)992') THEN 'Tadjikistan'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)993') THEN 'Turkménistan'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)994') THEN 'Azerbaïdjan'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)995') THEN 'Géorgie'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)996') THEN 'Kirghizistan'
            WHEN REGEXP_CONTAINS(REPLACE(REPLACE(CAST(`phone number` AS STRING), ' ', ''), '.', ''), r'^(?:\+|00)998') THEN 'Ouzbékistan'

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