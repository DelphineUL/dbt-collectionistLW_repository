with 

source as (

    select * from {{ source('lewagonxlecollectionist', 'Lead_Scoring') }}

),

renamed as (

    select
        deal_id_,
        contact_id,
        deal___client_contract_email___sha256,
        deal___deal_b2b2c,
        deal___deal_event,
        deal___static_segmentation_prospects_mkt,
        contact___create_date___monthly,
        contact___count_lc_sales_deals,
        contact___lc_number_of_primary,
        contact___m_a_import,
        contact___ip_country_code,
        contact___country_region,
        contact___city,
        phone_number,
        contact___preferred_language,
        deal___source,
        deal___app_inquiry,
        deal___static_latest_source,
        deal___deal_macro_source,
        deal___destination_inquiry,
        deal___final_booked_destination,
        deal___option_inquiry,
        deal___requested_houses,
        deal___final_commission_ht,
        deal___partner_s_commission,
        deal___budget,
        deal___budget_per_night,
        deal___deal_stage_macro,
        deal___deal_stage,
        deal___lost_reason,
        deal___number_of_sales_activities,
        deal___create_date___monthly,
        deal___close_date___monthly,
        deal___client_rental_amount,
        deal___time_to_reach_out,
        deal___days_to_close,
        deal___inquiry_check_in_date___monthly,
        deal___inquiry_check_out_date___monthly,
        deal___number_of_nights,
        deal___booking_window_inquiry,
        deal___final_check_in_date___monthly,
        deal___final_check_out_date___monthly,
        deal___stay_duration

    from source

)

select * from renamed