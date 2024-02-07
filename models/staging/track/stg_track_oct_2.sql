with

    track as (
        select

            `_id.ad_type` as ad_type,
            `_id.ssp` as ssp,
            `_id.platform_type` as platform_type,

            
            `_id.dealcode` as dealcode,
            `_id.zip` as pincode,

            `_id.make` as device_make,
            `_id.model` as device_model,
            `_id.device_type` as device_type,

            `_id.app_id` as app_id,
            `_id.category` as category,
            `_id.domain` as domain,
            `_id.app_name` as app_name,
            `_id.p_bundle` as p_bundle,

            `_id.date` as date,
            `_id.type` as type,
            `_id.floor_currency` as floor_currency,
            track_count,
            avg_floor_price,
            avg_bid_price

        from paytunes_data.track_oct10_16
    )

select *
from track
