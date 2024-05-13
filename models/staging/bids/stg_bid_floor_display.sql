with

    display_bids as (

        select

            `_id.ssp` as ssp,
            `_id.ad_type` as ad_type,

            lower(`_id.device_os`) as device_os,
            `_id.device_type` as device_type,

            `_id.pincode` as pincode,

            `_id.app_id` as ssp_app_id,
            `_id.app_name` as ssp_app_name,
            `_id.bundle` as bundle,
            `_id.publisher_id` as publisher_id,

            `_id.device_lang` as device_lang,
            split(`_id.category`, ',')[0] as category,

            `_id.pos[0]` as pos,
            cast(`_id.h[0]` as int) as h,
            cast(`_id.w[0]` as int) as w,
            -- `_id.topframe[0]` as topframe,
            cast(`_id.fmt[0][0][0]` as int) as fmt,
            cast(`_id.instl[0]` as int) as instl,  -- interstitial or full screen

            `_id.date` as date,
            cast(`_id.floor_price` as float) as fp,
            cast(bid_count as int) as bids

        from hive_metastore.paytunes_data.bid_floor_display

    )

select *
from
    display_bids

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
    
