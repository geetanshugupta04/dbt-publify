with

    mctv_bids as (

        select

            bid_device_make as device_make,
            bid_device_os as device_os,
            devie_geo_region as device_region,
            imp_instl,
            imp_bid_floor as bid_floor,
            imp_tag_id,
            imp_video_min_duration as min_duration,
            imp_video_max_duration as max_duration,
            imp_video_startdelay as start_delay,
            imp_video_w as video_width,
            imp_video_h as video_height,
            imp_video_placement as video_placement,
            imp_video_linearity as video_linearity,
            imp_video_maxextended as maxextended,
            imp_video_boxingallowed as boxingallowed,
            app_bundle,
            app_app_id,
            app_app_name,
            app_domain,
            app_publisher_id,
            app_publisher_name,
            app_publisher_domain,
            ssp,
            ad_type,
            device_type,
            platform_type,
            zip as pincode

        from {{ ref("stg_mctv_dec20_0_1") }}
    )

    

select *
from mctv_bids
