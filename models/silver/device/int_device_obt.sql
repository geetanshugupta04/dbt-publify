with

    bids as (
        select *
        from {{ ref("stg_bid_device") }}
        where make is not null and model is not null and device_os is not null
    -- and make = 'lava'
    ),

    vast as (
        select *
        from {{ ref("stg_vast_device") }}
        where make is not null and model is not null and device_os is not null
    -- and make = 'lava'
    ),

    track as (
        select *
        from {{ ref("stg_track_device") }}
        where make is not null and model is not null and device_os is not null
    -- and make = 'lava'
    ),

    device_os as (select * from {{ ref("stg_device_os_metadata") }}),

    device_master as (select * from {{ ref("int_device_master") }}),

    merged as (
        select
            b.date,
            b.ad_type,
            b.make,
            b.model,
            b.device_os,
            b.bid_requests,
            b.bid_response,
            v.bid_wins,
            t.impression,
            t.creative_view,
            t.clicks,
            t.complete
        from bids as b
        left join
            vast as v
            on b.date = v.date
            and b.ad_type = v.ad_type
            and b.make = v.make
            and b.model = v.model
            and b.device_os = v.device_os
        left join
            track as t
            on b.date = t.date
            and b.ad_type = t.ad_type
            and b.make = t.make
            and b.model = t.model
            and b.device_os = t.device_os
    ),

    merged_with_device_os as (
        select m.*, device_os.cleaned_device_os
        from merged as m
        left join device_os on m.device_os = device_os.device_os

    ),

    merged_with_device_master as (
        select
            m.*,
            master.device_id,
            master.raw_make,
            master.raw_model,
            master.company_make,
            master.master_model,
            master.device_type,
            master.release_month,
            master.release_year,
            master.cost

        from merged_with_device_os as m
        left join
            device_master as master
            on m.make = master.raw_make
            and m.model = master.raw_model
    ),

    final as (
        select
            date,
            ad_type,
            company_make,
            raw_make,
            master_model,
            raw_model,
            cleaned_device_os,
            device_type,
            release_month,
            release_year,
            cost,
            sum(bid_requests) as bid_requests,
            sum(bid_response) as bid_response,
            sum(bid_wins) as bid_wins,
            sum(impression) as impressions,
            sum(
                case when creative_view = 0 then impression else creative_view end
            ) as creative_view,
            sum(clicks) as clicks,
            sum(case when complete = 0 then impression else complete end) as complete
        from merged_with_device_master
        -- where device_make <> ''
        group by
            date,
            ad_type,
            company_make,
            raw_make,
            master_model,
            raw_model,
            cleaned_device_os,
            device_type,
            release_month,
            release_year,
            cost
    )

select *
from final


    -- metadata_deviceosmetadata
    -- device_os, cleaned_device_os
    -- metadata_devicecompany
    -- company_id, make
    -- metadata_devicemetadata
    -- make, model, device_id
    -- metadata_devicemaster
    -- company_id, device_id, model,
    -- device_type, release_month, release_year
    
