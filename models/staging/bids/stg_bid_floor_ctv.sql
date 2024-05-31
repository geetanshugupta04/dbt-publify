with

    ctv_bids as (

        select

            `_id.ssp` as ssp,
            `_id.ad_type` as ad_type,

            `_id.year` as year,
            `_id.month` as month,
            `_id.day` as day,
            `_id.hour` as hour,

            lower(`_id.device_os`) as device_os,
            `_id.device_type` as device_type,
            lower(`_id.make`) as make,
            lower(`_id.model`) as model,

            `_id.ip` as ip,
            `_id.ipv6` as ipv6,
            lower(`_id.ifa`) as ifa,
            `_id.pincode` as pincode,
            `_id.geo_long` as lon,
            `_id.geo_lat` as lat,

            `_id.app_id` as ssp_app_id,
            `_id.app_name` as ssp_app_name,
            `_id.bundle` as bundle,
            `_id.domain` as domain,
            `_id.publisher_id` as publisher_id,
            split(`_id.category`, ',')[0] as category,

            `_id.instl[0]` as instl,
            `_id.h[0]` as h,
            `_id.w[0]` as w,
            `_id.placement[0]` as placement,
            `_id.linearity[0]` as linearity,
            `_id.startdelay[0]` as startdelay,
            `_id.minduration[0]` as minduration,
            `_id.maxduration[0]` as maxduration,
            `_id.maxextended[0]` as maxextended,
            `_id.skip[0]` as skip,
            `_id.skipafter[0]` as skipafter,
            `_id.skipmin[0]` as skipmin,

            coalesce(
                `_id.content_app.livestream`, `_id.content_site.livestream`
            ) as livestream,
            coalesce(`_id.content_app.genre`, `_id.content_site.genre`) as genre,

            coalesce(
                `_id.content_app.contentrating`, `_id.content_site.contentrating`
            ) as contentrating,

            coalesce(
                `_id.content_app.ext.network`, `_id.content_site.ext.network`
            ) as network,
            coalesce(
                `_id.content_app.ext.channel`, `_id.content_site.ext.channel`
            ) as channel,
            coalesce(`_id.content_app.prodq`, `_id.content_site.prodq`) as prodq,

            coalesce(`_id.content_app.cat[0]`, `_id.content_site.cat[0]`) as cat0,
            coalesce(`_id.content_app.cat[1]`, `_id.content_site.cat[1]`) as cat1,
            coalesce(`_id.content_app.cat[2]`, `_id.content_site.cat[2]`) as cat2,
            coalesce(`_id.content_app.cat[3]`, `_id.content_site.cat[3]`) as cat3,
            coalesce(`_id.content_app.cat[4]`, `_id.content_site.cat[4]`) as cat4,

            `_id.dealcode[0][0]` as dealcode,
            coalesce(`_id.bidfloor[0][0]`, `_id.floor_price`) as floor_price,

            bid_count
        from {{ source("paytunes_data", "bid_floor_ctv") }}
    )

select *
from ctv_bids
