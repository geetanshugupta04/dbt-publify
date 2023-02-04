with
clicks as (

    select
    
    campaign_id as campaign_id_click,
    creative_id as creative_id_click,
    auction_id,
    win_price as win_price_click,
    bid_id as bid_id_click,
    created_on as created_on_click,
    auction_price_difference as auction_price_difference_click,
    traffic_source as traffic_source_click,
    buyer_id as buyer_id_click
    
    
     from {{ source('teleads_sources', 'clicks_trial')}}

)

select * from clicks