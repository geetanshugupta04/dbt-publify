with

    bids as (select * from {{ ref("stg_bid_device") }} 
    -- where make = 'lava'
    )
    ,

    vast as (select * from {{ ref("stg_vast_device") }} 
    -- where make = 'lava'
    )
    ,

    track as (select * from {{ ref("stg_track_device") }} 
    -- where make = 'lava'
    )
    ,

    

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
        select m.*, device_os.cleaned_device_os
        from merged as m
        left join device_os on m.device_os = device_os.device_os

    ),

    merged_with_device_raw as (
        select m.*, dr.device_id, dr.raw_make, dr.raw_model

        from merged_with_device_os as m
        left join
            device_raw as dr
            on m.make = dr.raw_make
            and m.model = dr.raw_model
    ),

    master_device as (

        select
            master.device_id,
            master.company_id,
            company.company_make,
            master.master_model,
            master.device_type,
            master.release_month,
            master.release_year,
            master.cost

        from device_master as master
        left join device_company as company on master.company_id = company.company_id

    ),

    merged_with_device_master as (
        select
            m.*,
            master.company_id,
            master.company_make,
            master.master_model,
            master.device_type,
            master.release_month,
            master.release_year,
            master.cost

        from merged_with_device_raw as m
        left join master_device as master on m.device_id = master.device_id

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

