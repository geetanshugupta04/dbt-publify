with

    audio_bids as (

        select

            -- top level
            `_id.ssp` as ssp,
            case when `_id.feed` = 3 then 'podcast' else `_id.ad_type` end as ad_type,
            `_id.dealcode[0][0]` as deal_0,
            -- `_id.dealcode[0][1]` as deal_1,
            -- `_id.dealcode[0][2]` as deal_2,
            -- `_id.dealcode[0][3]` as deal_3,

            -- temporal
            `_id.year` as year,  -- int
            `_id.month` as month,  -- int
            `_id.day` as day,  -- int
            `_id.hour` as hour,  -- int

            -- device
            lower(`_id.device_os`) as device_os,
            `_id.device_type` as device_type,  -- int
            -- lower(`_id.make`) as make,
            coalesce(`_id.model`, 'NA') as make,
            lower(`_id.model`) as model,

            -- user and location
            `_id.ip` as ip,
            `_id.ipv6` as ipv6,
            lower(`_id.ifa`) as ifa,
            `_id.pincode` as pincode,  -- int

            -- app and publisher
            `_id.app_id` as ssp_app_id,
            `_id.app_name` as ssp_app_name,
            `_id.bundle` as bundle,
            `_id.domain` as domain,
            `_id.publisher_id` as publisher_id,
            split(`_id.category`, ',')[0] as category,
            coalesce(`_id.genre_site`, `_id.genre_app`) as genre,
            coalesce(`_id.series_site`, 'NA') as series,

            -- metrics
            `_id.floor_price` as fp,  -- double
            `_id.bid_status` as bid_status,
            `_id.bid_price` as bp,  -- double
            bid_count as bids  -- int

        from {{ source("paytunes_data", "bid_floor_price_audio") }}
            -- where deal_0 not in (1, 2)
    )

select *
from audio_bids
