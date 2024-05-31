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
            'NA' as make,
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
            coalesce(`_id.genre_site`, `_id.genre_app`) as genre,
            -- `_id.series` as series,

            -- audio settings
            `_id.minduration[0]` as minduration,
            `_id.maxduration[0]` as maxduration,
            `_id.maxseq[0]` as maxseq,
            -- `_id.maxextended[0]` as maxextended,
            `_id.stitched[0]` as stitched,
            `_id.startdelay[0]` as startdelay,

            -- companion banner
            `_id.companionad[0][0].h` as companionad0h,
            `_id.companionad[0][1].h` as companionad1h,
            `_id.companionad[0][2].h` as companionad2h,
            `_id.companionad[0][3].h` as companionad3h,
            `_id.companionad[0][0].w` as companionad0w,
            `_id.companionad[0][1].w` as companionad1w,
            `_id.companionad[0][2].w` as companionad2w,
            `_id.companionad[0][3].w` as companionad3w,

            -- metrics
            `_id.floor_price` as fp,  -- double
            bid_count as bids  -- int

        from {{ source("paytunes_data", "bid_floor_audio") }}
        -- where deal_0 not in (1, 2)

    )

select *
from audio_bids
