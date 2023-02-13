with
bid_requests as (

    select

        auction_id,
        created_on as created_on_bid,
        url,
        device_ua,
        device_lat,
        device_lon,
        site_domain,
        device_os,
        site_domain_category,
        geometry,
        state,
        district
    
    
     from {{ source('teleads_sources', 'bids_trial')}}

)

select * from bid_requests