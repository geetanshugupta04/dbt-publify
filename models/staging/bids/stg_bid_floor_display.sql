with

    display_bids as (

        select

            -- top level
            `_id.ssp` as ssp,
            `_id.ad_type` as ad_type,

            -- temporal
            `_id.year` as year,  -- int
            `_id.month` as month,  -- int
            `_id.day` as day,  -- int
            `_id.hour` as hour,  -- int

            -- device
            lower(`_id.device_os`) as device_os,
            `_id.device_type` as device_type,  -- int
            lower(`_id.make`) as make,
            lower(`_id.model`) as model,

            -- user and location
            `_id.ip` as ip,
            `_id.ipv6` as ipv6,
            lower(`_id.ifa`) as ifa,
            `_id.pincode` as pincode,  -- int
            `_id.device_lon` as lon,  -- double
            `_id.device_lat` as lat,  -- double

            -- app and publisher
            `_id.app_id` as ssp_app_id,
            `_id.app_name` as ssp_app_name,
            `_id.bundle` as bundle,
            `_id.domain` as domain,
            `_id.publisher_id` as publisher_id,
            split(`_id.category`, ',')[0] as category,

            -- banner display options
            `_id.pos[0]` as pos,  -- int
            `_id.h[0]` as h,  -- int
            `_id.w[0]` as w,  -- int
            `_id.topframe[0]` as topframe,  -- int
            -- cast(`_id.instl[0]` as int) as instl,  -- interstitial or full screen
            
            -- metrics
            `_id.floor_price` as fp,  -- double
            bid_count as bids  -- int

        from {{ source("paytunes_data", "bid_floor_display") }}

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
    
