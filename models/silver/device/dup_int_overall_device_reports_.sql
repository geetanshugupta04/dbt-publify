with

    bids as (select * from {{ ref("stg_bid_device") }}),

    vast as (select * from {{ ref("stg_vast_device") }}),

    track as (select * from {{ ref("stg_track_device") }})

    device_os as (select * from {{ ref("stg_device_os_metadata") }})

    device_master as (select * from {{ ref("int_device_master") }})


    select * from bids


    /*
    bids_vast as (

        select * from bids 
    )
    select * 



    
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
            and lower(b.ad_type) = lower(v.ad_type)
            and lower(b.make) = lower(v.make)
            and lower(b.model) = lower(v.model)
            and lower(b.device_os) = lower(v.device_os)
        left join
            track as t
            on b.date = t.date
            and lower(b.ad_type) = lower(t.ad_type)
            and lower(b.make) = lower(t.make)
            and lower(b.model) = lower(t.model)
            and lower(b.device_os) = lower(t.device_os)
    ),

    merged_with_device_os as (
        select m.*, dos.cleaned_device_os,

        from merged as m
        left join device_os as dos on lower(m.device_os) = lower(dos.device_os)

    ),

    merged_with_device_master as (
        select
            m.*,
            dm.device_make,
            dm.device_model,
            dm.company_make,
            dm.master_model,
            dm.device_type,
            dm.release_month,
            dm.release_year,
            dm.cost
        from merged as m
        left join
            device_master as dm
            on lower(m.make) = lower(dm.device_make)
            and lower(m.model) = lower(dm.device_model)
    ),

    final as (
        select
            date,
            company_make,
            -- device_make,
            master_model,
            -- device_model,
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
        from merged_with_device_data
        where device_make <> ''
        group by
            date,
            company_make,
            -- -- 	device_make,
            master_model,
            -- device_model,
            cleaned_device_os,
            device_type,
            release_month,
            release_year,
            cost
    )

select *
from
    final

    -- metadata_deviceosmetadata
    -- device_os, cleaned_device_os
    -- metadata_devicecompany
    -- company_id, make
    -- metadata_devicemetadata
    -- make, model, device_id
    -- metadata_devicemaster
    -- company_id, device_id, model,
    -- device_type, release_month, release_year
    
*/