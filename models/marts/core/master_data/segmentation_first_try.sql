with 
merged_data as (

    select * from {{ ref('int_bids_imp_clicks')}}
),

subsetted_data as (

    select *

    from analytics.dbt_teleads_marts.int_bids_imp_clicks
    inner join 
        (
            select buyer_id_bid, count(created_on_bid) as count1
            from analytics.dbt_teleads_marts.int_bids_imp_clicks
            group by buyer_id_bid
            order by count1 desc
            limit 10
        )
    using(buyer_id_bid)

)

select 

	buyer_id_bid as buyer_id, 
    cast(created_on_bid as date) as date, 
    hour(created_on_bid) as hour,
    minute(created_on_bid) as minute,
    created_on_bid as time,
    state, 
    district,
    site_domain_category,
    device_os,
    count(created_on_bid) as total_bid_requests,
    count(created_on_imp) as total_impressions,
    count(created_on_click) as total_clicks

from subsetted_data

group by buyer_id, date, hour, minute, time, state, district, site_domain_category, device_os
