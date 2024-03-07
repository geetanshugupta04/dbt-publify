with

    bids as (
        select
            `_id.ssp` as ssp,
            `_id.ad_type` as ad_type,
            `_id.feed` as feed,
            `_id.device_os` as device_os,
            `_id.make` as make,
            `_id.model` as model,
            `_id.dealcode_0__0_` as dealcode,
            `_id.ip` as ip,
            `_id.ifa` as ifa,
            `_id.user_id` as uid,
            `_id.app_id` as app_id,
            `_id.app_name` as app_name,
            `_id.bundle` as bundle,
            `_id.publisher_id` as publisher_id,
            `_id.publisher_name` as publisher_name,
            `_id.floor_currency` as floor_currency,
            `_id.floor_price` as floor_price,
            `_id.category` as category,
            `_id.date` as date,
            bid_count

        from paytunes_data.adswizz_us_march4

    )

select *
from bids
