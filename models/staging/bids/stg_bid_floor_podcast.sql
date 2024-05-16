with

    audio_podcast_bids as (

        select

            `_id.ssp` as ssp,
            case when `_id.feed` = 3 then 'podcast' else `_id.ad_type` end as ad_type,
            `_id.feed` as feed,
            lower(`_id.device_os`) as device_os,
            `_id.pincode` as pincode,
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

            cast(`_id.maxseq[0]` as int) as maxseq,
            `_id.maxextended[0]` as maxextended,
            cast(`_id.minduration[0]` as int) as minduration,
            cast(`_id.maxduration[0]` as int) as maxduration,

            `_id.dealcode[0][0]` as deal_0,
            `_id.dealcode[0][1]` as deal_1,
            `_id.dealcode[0][2]` as deal_2,
            `_id.dealcode[0][3]` as deal_3,
            lower(`_id.model`) as model,
            lower(`_id.make`) as make,
            `_id.stitched[0]` as stitched,
            `_id.companionad[0][0].h` as companionad0h,
            `_id.companionad[0][1].h` as companionad1h,
            `_id.companionad[0][2].h` as companionad2h,
            `_id.companionad[0][3].h` as companionad3h,
            `_id.companionad[0][0].w` as companionad0w,
            `_id.companionad[0][1].w` as companionad1w,
            `_id.companionad[0][2].w` as companionad2w,
            `_id.companionad[0][3].w` as companionad3w,
            cast(`_id.startdelay[0]` as int) as startdelay,
            coalesce(`_id.genre_site`, `_id.genre_app`) as genre,

            coalesce(`_id.series_site`, `_id.series_app`) as series,

            cast(`_id.floor_price` as float) as fp,

            case
                when `_id.ssp` = 'Adswizz' then '2024-05-078' else `_id.date`
            end as date,

            bid_count

        from hive_metastore.paytunes_data.bid_floor_audio_podcast

    )

select *
from audio_podcast_bids
where deal_0 not in (1, 2)
