with
impressions as (

    select
    campaign_id as campaign_id_imp,
    creative_id as creative_id_imp,
    auction_id,
    win_price as win_price_imp,
    bid_id as bid_id_imp,
    created_on as created_on_imp,
    auction_price_difference as auction_price_difference_imp,
    traffic_source as traffic_source_imp,
    buyer_id as buyer_id_imp
    
    from {{ source('teleads_sources', 'impressions_trial')}}

)

select * from impressions