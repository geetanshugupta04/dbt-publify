with

    audio_bids as (

        select

            `_id.ssp` as ssp,
            `_id.ad_type` as ad_type,
            `_id.feed` as feed,
            `_id.device_os` as device_os,
            `_id.pincode` as pincode,
            `_id.app_id` as app_id,
            `_id.app_name` as app_name,
            `_id.bundle` as bundle,
            `_id.domain` as domain,
            `_id.publisher_id` as publisher_id,
            `_id.floor_price` as floor_price,
            `_id.category` as category,
            `_id.date` as date,
            `_id.maxseq[0]` as maxseq,
            `_id.maxextended[0]` as maxextended,
            `_id.minduration[0]` as minduration,
            `_id.maxduration[0]` as maxduration,
            `_id.series` as series,
            `_id.dealcode[0][0]` as deal_0,
            `_id.dealcode[0][1]` as deal_1,
            `_id.dealcode[0][2]` as deal_2,
            `_id.dealcode[0][3]` as deal_3,
            `_id.model` as model,
            `_id.make` as make,
            `_id.stitched[0]` as stitched,
            `_id.companionad[0][0].h` as companionad0h,
            `_id.companionad[0][1].h` as companionad1h,
            `_id.companionad[0][2].h` as companionad2h,
            `_id.companionad[0][3].h` as companionad3h,
            `_id.companionad[0][0].w` as companionad0w,
            `_id.companionad[0][1].w` as companionad1w,
            `_id.companionad[0][2].w` as companionad2w,
            `_id.companionad[0][3].w` as companionad3w,
            `_id.startdelay[0]` as startdelay,
            `_id.genre_site` as genre_site,
            `_id.genre_app` as genre_app,

            bid_count

        from hive_metastore.paytunes_data.bid_floor_audio

    ),

    audio_bids_cleaned as (

        select
            ssp,
            case when feed = 3 then 'podcast' else ad_type end as ad_type,
            lower(device_os) as device_os,

            lower(model) as model,
            lower(make) as make,

            pincode,
            app_id as ssp_app_id,
            app_name as ssp_app_name,
            bundle,
            domain,
            publisher_id,
            split(category, ',')[0] as category,

            deal_0,
            deal_1,
            deal_2,
            deal_3,

            maxseq,
            maxextended,
            minduration,
            maxduration,
            series,
            stitched,
            companionad0h,
            companionad1h,
            companionad2h,
            companionad3h,
            companionad0w,
            companionad1w,
            companionad2w,
            companionad3w,
            startdelay,

            coalesce(genre_site, genre_app) as genre,
            floor_price,
            date,

            bid_count as bids

        from audio_bids

    )

select *
from audio_bids_cleaned
where deal_0 not in (1, 2)
