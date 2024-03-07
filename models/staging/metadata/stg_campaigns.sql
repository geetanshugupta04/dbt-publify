with

    -- line_items_with_deals as (
    --     select line_item_id 
    --     from paytunes_data.track_pub_hourly_feb_2023
    --     group by 1),
    
    -- campaigns_with_deals_and_everything as (
         
    --     select
    --         id as line_item_id,
    --         campaign_id

    --     from paytunes_data.campaign_lineitem
    --     where id in (select line_item_id from line_items_with_deals)
    --     group by 1,2
    -- ),

    campaigns as (
        select
            id as campaign_id,
            name as campaign_name,
            min(start_date) as campaign_start_date,
            max(end_date) as campaign_end_date,
            show_avg_cpm

        from paytunes_data.campaign_campaign
        -- where id in (select campaign_id from campaigns_with_deals_and_everything)
        group by id, campaign_name, show_avg_cpm
    )

select campaign_id
from campaigns