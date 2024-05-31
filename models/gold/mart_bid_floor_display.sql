with

    display_bids as (select * from {{ ref("int_bid_floor_display") }}),

    cleaned_bids as (

        select

            ssp,
            ad_type,

            cleaned_device_os,
            -- case
            -- when device_type is null or device_type = '1'
            -- then 'Mobile/Tablet'
            -- when device_type = '2'
            -- then 'PC'
            -- else 'Unknown'
            -- end as 
            device_type,
            final_make,
            final_model,

            ip,
            ipv6,
            ifa,

            lon,
            lat,
            city,
            state,

            ssp_app_id,
            ssp_app_name,
            bundle,
            publisher_id,

            publify_app as app_final,
            publify_publisher as publisher_final,

            app_category_tag,
            iab_category_name,
            itunes_category,

            /*
pos
0 unknown
1 above the fold
3 below the fold
4 header
5 footer
6 sidebar
7 full screen

*/
            case
                when pos = '3'
                then 'Below the fold'
                when pos = '1'
                then 'Above the fold'
                else 'Unknown'
            end as pos,
            h,
            w,
            topframe,
            -- case
            -- when instl = 1
            -- then 'Interstitial'
            -- when instl = 0
            -- then 'Not Interstitial'
            -- else 'Unknown'
            -- end as instl,
            fp,
            case when fp is null then 1 else 0 end as null_fps,
            sum(bids) as bids

        from display_bids
        where device_type not in ('3', '8')

        group by all

    ),

    big_pubs_apps as (
        select
            bids.*,
            sum(bids) over (
                partition by publisher_id, null_fps
            ) as pub_bids_sum_not_null,  -- sum_nulls get removed in the qualify clause
            sum(bids) over (
                partition by ssp_app_name, null_fps
            ) as ssp_app_bids_sum_not_null,  -- sum_nulls get removed in the qualify clause
            sum(bids) over (
                partition by app_final, null_fps
            ) as pub_app_bids_sum_not_null,  -- sum_nulls get removed in the qualify clause
            sum(bids) over (partition by publisher_id) as pub_bids_sum,
            sum(bids) over (partition by ssp_app_name) as ssp_app_bids_sum,
            sum(bids) over (partition by app_final) as pub_app_bids_sum,
            sum(bids) over () as total_bids_sum

        from cleaned_bids as bids
        where app_final not in ('ne')
        qualify (pub_app_bids_sum > 100000) and null_fps = 0 and app_final is not null

    )

select *
from big_pubs_apps
