with

    bids_display as (

        select

            `_id.ssp` as ssp,
            `_id.ad_type` as ad_type,
            `_id.feed` as feed,
            `_id.dealcode[0][0]` as dealcode,
            `_id.device_os` as device_os,
            `_id.pincode` as pincode,
            `_id.app_id` as app_id,
            `_id.app_name` as app_name,
            `_id.bundle` as bundle,
            `_id.publisher_id` as publisher_id,
            `_id.category` as category,
            `_id.date` as date,
            `_id.pos[0]` as banner_position,
            `_id.h[0]` as banner_height,
            `_id.w[0]` as banner_width,
            `_id.topframe[0]` as banner_topframe,
            `_id.fmt[0][0][0]` as banner_fmt,

            `_id.floor_price` as fp,
            bid_count

        from paytunes_data.bid_floor_display

    ),

    bids_display_grouped as (

        select

            date,
            ssp,
            ad_type,
            dealcode,
            device_os,
            pincode,
            app_id as ssp_app_id,
            app_name as ssp_app_name,
            bundle,
            publisher_id,
            category,
            banner_position,
            banner_topframe,
            banner_height,
            banner_width,
            banner_fmt,
            fp,
            sum(bid_count) as bids

        from bids_display
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17

    )

select *
from bids_display_grouped
