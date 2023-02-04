with
impressions as (

    select
    campaign_id,
    creative_id,
    auction_id,
    win_price,
    bid_id,
    created_on,
    auction_price_difference,
    traffic_source,
    buyer_id
    
    from {{ source('teleads_sources', 'impressions_trial')}}

)

select * from impressions