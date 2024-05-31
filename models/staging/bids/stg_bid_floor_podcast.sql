with

    podcast_bids as (

        select

            `_id.ssp` as ssp,
            case when `_id.feed` = 3 then 'podcast' else `_id.ad_type` end as ad_type,
            `_id.feed` as feed,  -- int

            lower(`_id.device_os`) as device_os,
            lower(`_id.model`) as model,
            lower(`_id.make`) as make,

            `_id.ip` as ip,
            `_id.ifa` as ifa,

            `_id.pincode` as pincode,  -- int
            `_id.app_id` as ssp_app_id,
            `_id.app_name` as ssp_app_name,
            `_id.bundle` as bundle,
            `_id.domain` as domain,
            `_id.publisher_id` as publisher_id,
            case
                when coalesce(`_id.series_site`, `_id.series_app`) = 'DHADKANE MERI SUN'
                then 373
                else split(`_id.category`, ',')[0]
            end as category,

            `_id.maxseq[0]` as maxseq,  -- int 
            `_id.maxextended[0]` as maxextended,  -- int
            `_id.minduration[0]` as minduration,  -- int
            `_id.maxduration[0]` as maxduration,  -- int

            `_id.dealcode[0][0]` as deal_0,
            `_id.dealcode[0][1]` as deal_1,
            `_id.dealcode[0][2]` as deal_2,
            `_id.dealcode[0][3]` as deal_3,

            `_id.stitched[0]` as stitched,  -- int
            `_id.companionad[0][0].h` as companionad0h,  -- int
            `_id.companionad[0][1].h` as companionad1h,  -- int
            `_id.companionad[0][2].h` as companionad2h,  -- int
            `_id.companionad[0][3].h` as companionad3h,  -- int
            `_id.companionad[0][0].w` as companionad0w,  -- int
            `_id.companionad[0][1].w` as companionad1w,  -- int
            `_id.companionad[0][2].w` as companionad2w,  -- int
            `_id.companionad[0][3].w` as companionad3w,  -- int
            `_id.startdelay[0]` as startdelay,  -- int
            coalesce(`_id.genre_site`, `_id.genre_app`) as genre,

            coalesce(`_id.series_site`, `_id.series_app`) as series,

            `_id.floor_price` as fp,  -- double

            `_id.date` as date,

            bid_count

        from hive_metastore.paytunes_data.bid_floor_podcast

    )

select *
from podcast_bids
where deal_0 not in (1, 2) and ad_type = 'podcast'
