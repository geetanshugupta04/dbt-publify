with

    bids as (select * from {{ ref("int_pincode_merge") }}),

    ssp_apps as (select * from {{ ref("int_ssp_apps") }}),

    ssp_publishers as (select * from {{ ref("int_ssp_publishers") }}),

    clean_bids as (

        select

            ssp,
            case when feed = '3' then 'podcast' else ad_type end as ad_type,
            platform_type,
            -- feed,
            ssp_app_id,
            ssp_app_name,
            publisher_id,
            bundle,
            domain,
            -- banner_height,
            -- banner_width,
            -- min_duration,
            -- max_duration,
            -- cleaned_device_os_2 as cleaned_device_os,
            avg_floor_price,
            -- city,
            -- state,
            sum(bid_count) as bid_counts

        from bids

        group by 1, 2, 3, 4, 5, 6, 7, 8, 9

    ),

    clean_ssp_apps as (
        select ssp_app_name, ssp_app_id, bundle, publisher_id, publify_app_name

        from ssp_apps
    ),

    apps_merged as (

        select clean_bids.*, apps.publify_app_name

        from clean_bids
        left join
            clean_ssp_apps as apps
            on clean_bids.ssp_app_id = apps.ssp_app_id
            and clean_bids.ssp_app_name = apps.ssp_app_name
            and clean_bids.bundle = apps.bundle
            and clean_bids.publisher_id = apps.publisher_id

    ),

    clean_ssp_publishers as (
        select
            ssp_publisher_id,
            ssp_publisher_name,
            ssp,
            publify_ssp_publisher_name,
            publify_ssp_publisher_master_id

        from ssp_publishers
    ),

    apps_pubs_merged as (

        select

            bids.*,
            ssp_publisher_id,
            ssp_publisher_name,
            publify_ssp_publisher_name,
            publify_ssp_publisher_master_id

        from apps_merged as bids
        left join
            clean_ssp_publishers as pubs
            on bids.publisher_id = pubs.ssp_publisher_id
            and bids.ssp = pubs.ssp

    )

select *
from
    apps_pubs_merged
    -- order by bid_counts desc
    
