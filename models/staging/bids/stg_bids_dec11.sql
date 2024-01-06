with
    bids as (

        select

            `_id.ssp` as ssp, -- already clean
            `_id.ad_type` as ad_type, -- already clean
            `_id.feed` as feed, -- if feed = 3 then ad_type = podcast
            `_id.ssp_app_id` as ssp_app_id,
            `_id.ssp_app_name` as ssp_app_name,
            `_id.bundle` as bundle,
            `_id.domain` as domain,
            `_id.platform_type` as platform_type, -- already clean
            `_id.publisher_id` as publisher_id,
            `_id.pincode` as pincode,
            `_id.device_os` as device_os,
            `_id.banner_height` as banner_height,
            `_id.banner_width` as banner_width,
            `_id.min_duration` as min_duration,
            `_id.max_duration` as max_duration,
            cast(`bidcount` as int) as bid_count,
            cast(`averageFloorPrice` as float) as avg_floor_price

        from paytunes_data.bids_dec11

    )

select *
from bids