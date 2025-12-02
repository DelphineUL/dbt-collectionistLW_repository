with 

source as (

    select * from {{ source('lewagonxlecollectionist', 'Keyword') }}

),

renamed as (

    select
        keyword,
        destination,
        currency,
        segmentation,
        avg__monthly_searches,
        three_month_change,
        yoy_change,
        competition,
        competition__indexed_value_,
        top_of_page_bid__low_range_,
        top_of_page_bid__high_range_,
        ad_impression_share,
        organic_average_position,
        organic_impression_share,
        in_account,
        searches__nov_2021,
        searches__dec_2021,
        searches__jan_2022,
        searches__feb_2022,
        searches__mar_2022,
        searches__apr_2022,
        searches__may_2022,
        searches__jun_2022,
        searches__jul_2022,
        searches__aug_2022,
        searches__sep_2022,
        searches__oct_2022,
        searches__nov_2022,
        searches__dec_2022,
        searches__jan_2023,
        searches__feb_2023,
        searches__mar_2023,
        searches__apr_2023,
        searches__may_2023,
        searches__jun_2023,
        searches__jul_2023,
        searches__aug_2023,
        searches__sep_2023,
        searches__oct_2023,
        searches__nov_2023,
        searches__dec_2023,
        searches__jan_2024,
        searches__feb_2024,
        searches__mar_2024,
        searches__apr_2024,
        searches__may_2024,
        searches__jun_2024,
        searches__jul_2024,
        searches__aug_2024,
        searches__sep_2024,
        searches__oct_2024,
        searches__nov_2024,
        searches__dec_2024,
        searches__jan_2025,
        searches__feb_2025,
        searches__mar_2025,
        searches__apr_2025,
        searches__may_2025,
        searches__jun_2025,
        searches__jul_2025,
        searches__aug_2025,
        searches__sep_2025,
        searches__oct_2025

    from source

)

select * from renamed