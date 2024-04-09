with

    bids as (
        select *
        from {{ ref("int_bids_gaming_analysis") }}
        where
            -- (ifa is not null or ip is not null) and 
            date = '2024-03-11'

    ),

    device_metadata as (select * from {{ ref("int_device_master") }}),

    gaming_categories as (

        select
            bids.*,
            -- case when ifa is null then ip else ifa end as user_id,
            case
                when dealcode ilike '%gaming%' or app_category_tag = 'Gaming'
                then 1
                when iab_category ilike '%game%' and iab_category ilike '%gaming%'
                then 1
                else 0
            end as gaming_category

        from bids

    ),

    merged_with_device_data as (
        select
            bids.*,
            d.company_make,
            d.master_model,
            d.device_type,
            d.release_month,
            d.release_year,
            d.cost
        from gaming_categories as bids
        left join
            device_metadata as d on bids.make = d.raw_make and bids.model = d.raw_model
    ),

    bids_grouped as (

        select
            date,
            ssp,
            ad_type,
            device_os,
            company_make,
            master_model,
            cost as device_cost,
            ip,
            ifa,
            uid,
            case when ifa is null then uid else ifa end as user_id,
            ssp_app_name,
            publify_app_name,
            app_category_tag,
            iab_category,
            gaming_category,

            sum(bids) as bids

        from merged_with_device_data
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16

    )

select *
from bids_grouped