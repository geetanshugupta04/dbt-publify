with

    mctv_bids as (

        select
        
            `_id.ssp` as ssp,
            `_id.ad_type` as ad_type,
            `_id.device_type` as device_type,
            `_id.date` as date,
            `_id.instl[0]` as instl,
            cast(`_id.h[0]` as int) as h,
            cast(`_id.w[0]` as int) as w,
            `_id.placement[0]` as placement,
            `_id.startdelay[0]` as startdelay,
            cast(`_id.minduration[0]` as int) as minduration,
            cast(`_id.maxduration[0]` as int) as maxduration,
            `_id.linearity[0]` as linearity,
            cast(bid_count as int) as bids

        from hive_metastore.paytunes_data.mctv_may5
    )

select *
from mctv_bids
