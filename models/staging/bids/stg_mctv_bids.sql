with

    mctv_bids as (

        select
            `_id.ssp` as ssp,
            `_id.ad_type` as ad_type,
            lower(`_id.device_os`) as device_os,
            lower(`_id.make`) as make,
            lower(`_id.model`) as model,
            `_id.devicetype` as device_type,
            `_id.ifa` as ifa,
            `_id.pincode` as pincode,
            `_id.lat` as lat,
            `_id.lon` as lon,
            `_id.app_id` as ssp_app_id,
            `_id.app_name` as ssp_app_name,
            `_id.bundle` as bundle,
            `_id.publisher_id` as publisher_id,
            `_id.category` as category,
            `_id.date` as date,
            `_id.ip` as ip,
            bid_count

        from hive_metastore.paytunes_data.bids_mctv
    )

select *
from mctv_bids
