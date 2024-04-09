with

    device_data as (

        select
            `_id.ad_type` as ad_type,
            `_id.feed` as feed,
            `_id.device_os` as device_os,
            `_id.make` as make,
            `_id.model` as model,
            `_id.ua` as ua,
            bid_count

        from hive_metastore.paytunes_data.device_data_ua
    )

select *
from device_data
