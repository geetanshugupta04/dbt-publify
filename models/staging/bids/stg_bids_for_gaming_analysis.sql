with

    bids as (
        select
            `_id.ssp` as ssp,
            `_id.ad_type` as ad_type,
            `_id.feed` as feed,
            `_id.device_os` as device_os,
            `_id.make` as make,
            `_id.model` as model,
            `_id.dealcode_0__0_` as deal_0,
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

        from hive_metastore.paytunes_data.adswizz_us_march4

    ),

    cleaned_bids as (

        select
            ssp,
            case when feed = 3 then 'podcast' else ad_type end as ad_type,
            lower(device_os) as device_os,
            lower(make) as make,
            lower(model) as model,
            case when deal_0 is null then 'NA' else deal_0 end as deal_0,
            ip,
            ifa,
            uid,
            app_id as ssp_app_id,
            app_name as ssp_app_name,
            bundle,
            publisher_id,
            publisher_name,
            floor_price as fp,
            split(category, ',')[0] as category,
            date,
            bid_count as bids

        from bids

    )

select *
from cleaned_bids
