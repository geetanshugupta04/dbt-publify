with

    video_bids as (

        select
            `_id.ssp` as ssp,
            `_id.ad_type` as ad_type,
            lower(`_id.device_os`) as device_os,
            lower(`_id.make`) as make,
            lower(`_id.model`) as model,
            `_id.pincode` as pincode,
            `_id.app_id` as ssp_app_id,
            `_id.app_name` as ssp_app_name,
            `_id.bundle` as bundle,
            `_id.domain` as domain,
            `_id.publisher_id` as publisher_id,
            split(`_id.category`, ',')[0] as category,
            `_id.date` as date,
            cast(`_id.floor_price` as float) as fp,
            `_id.tagid[0]` as tag_id,
            cast(`_id.h[0]` as int) as h,
            cast(`_id.w[0]` as int) as w,
            `_id.placement[0]` as placement,
            cast(`_id.skipafter[0]` as int) as skipafter,
            cast(`_id.skip[0]` as int) as skip,
            cast(`_id.skipmin[0]` as int) as skipmin,

            cast(`_id.startdelay[0]` as int) as startdelay,
            cast(`_id.minduration[0]` as int) as minduration,
            cast(`_id.maxduration[0]` as int) as maxduration,
            cast(`_id.linearity[0]` as int) as linearity,

            `_id.companion_banner_h[0][0]` as companion_banner_h0,
            `_id.companion_banner_h[0][1]` as companion_banner_h1,
            `_id.companion_banner_h[0][2]` as companion_banner_h2,
            `_id.companion_banner_h[0][3]` as companion_banner_h3,
            `_id.companion_banner_w[0][0]` as companion_banner_w0,
            `_id.companion_banner_w[0][1]` as companion_banner_w1,
            `_id.companion_banner_w[0][2]` as companion_banner_w2,
            `_id.companion_banner_w[0][3]` as companion_banner_w3,

            `_id.companion_banner_h[0][4]` as companion_banner_h4,
            `_id.companion_banner_h[0][5]` as companion_banner_h5,

            `_id.companion_banner_h[0][6]` as companion_banner_h6,
            `_id.companion_banner_h[0][7]` as companion_banner_h7,
            `_id.companion_banner_w[0][4]` as companion_banner_w4,
            `_id.companion_banner_w[0][5]` as companion_banner_w5,
            `_id.companion_banner_w[0][6]` as companion_banner_w6,
            `_id.companion_banner_w[0][7]` as companion_banner_w7,
            bid_count

        from hive_metastore.paytunes_data.bid_floor_video
    )

select *
from video_bids
