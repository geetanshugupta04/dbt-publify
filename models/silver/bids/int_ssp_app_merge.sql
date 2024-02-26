with

    bids as (select * from {{ ref("int_pincode_merge") }}),

    ssp_apps as (select * from {{ ref("int_ssp_apps") }}),

    clean_bids as (

        select

            -- ssp,
            -- case when feed = '3' then 'podcast' else ad_type end as ad_type,
            -- platform_type,
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
            -- city,
            -- state,
            sum(bid_count) as bid_counts
        -- avg_floor_price
        from bids
        group by 1, 2, 3, 4, 5
        having bid_counts > 100000
    ),

    clean_ssp_apps as (
        select
            -- ssp,
            -- platform_type,
            ssp_app_name, ssp_app_id, bundle, domain, publisher_id, publify_app_name
        -- publisher_group
        from ssp_apps
    ),

    merged as (

        select
            clean_bids.*,
            -- apps.publify_publisher_id,
            apps.publify_app_name
        -- apps.publisher_group
        from clean_bids
        left join
            clean_ssp_apps as apps
            -- clean_bids.ssp = apps.ssp
            -- and 
            -- clean_bids.platform_type = apps.platform_type
            -- and 
            on clean_bids.ssp_app_id = apps.ssp_app_id
            and clean_bids.ssp_app_name = apps.ssp_app_name
            and clean_bids.bundle = apps.bundle
            -- and clean_bids.domain = apps.domain
            and clean_bids.publisher_id = apps.publisher_id

    )

select *
from merged
order by bid_counts desc
