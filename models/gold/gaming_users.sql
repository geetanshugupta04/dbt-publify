with

    bids as (
        select * from {{ ref("int_bids_gaming_analysis") }}
    -- where
    -- -- (ifa is not null or ip is not null) and 
    -- date = '2024-03-11'
    ),

    gaming_categories as (

        select
            bids.*,
            -- case when ifa is null then ip else ifa end as user_id,
            case
                when deal_0 ilike '%gaming%' or app_category_tag = 'Gaming'
                then 1
                when iab_category ilike '%game%' and iab_category ilike '%gaming%'
                then 1
                else 0
            end as gaming_category

        from bids

    ),

    bids_grouped as (

        select

            ssp,
            ad_type,

            ip,
            ifa,

            deal_0,
            age,
            gender,

            cleaned_device_os,
            final_make,
            final_model,
            device_cost,

            publify_app_name,
            publisher_final,

            app_category_tag,
            iab_category,
            gaming_category,

            fp,
            sum(bids) as bids

        from gaming_categories
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17

    )

select *
from bids_grouped
