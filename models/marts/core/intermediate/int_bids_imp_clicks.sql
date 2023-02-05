with 
bid_requests as (

    select * from {{ ref('stg_bid_requests')}}
),

impressions as (

    select * from {{ ref('stg_impressions')}}
),

clicks as (

    select * from {{ ref('stg_clicks')}}
),

joined as (

    select *
    from bid_requests as bids
    left join impressions as imps 
    using(auction_id)
    left join clicks
    using(auction_id)

)

select * from joined