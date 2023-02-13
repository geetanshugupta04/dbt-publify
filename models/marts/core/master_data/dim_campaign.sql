with bids_imp_clicks as (

    select * from {{ ref('int_bids_imp_clicks')}}
),

overall_reporting as (

    select
 
    cast(created_on_bid as date) as date, 
    hour(created_on_bid) as hour,
    campaign_id_imp,
    count(created_on_bid) as total_bid_requests,
    count(created_on_imp) as total_impressions,
    count(created_on_click) as total_clicks,
    sum(win_price_imp / 1000) as total_cost,
    avg(win_price_imp) as avg_cost_per_imp,
    total_impressions / total_bid_requests as impression_rate

    from bids_imp_clicks
    group by campaign_id_imp, date, hour

)

select * from overall_reporting