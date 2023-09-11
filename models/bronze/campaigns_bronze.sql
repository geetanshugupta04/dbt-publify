with
    campaigns as (
        select
            campaign_id,
            campaign_name,
            campaign_created_at,
            min(start_date) as campaign_start_date,
            max(end_date) as campaign_end_date,
            show_avg_cpm

        from {{ref("campaigns")}}
        group by campaign_id, campaign_name, campaign_created_at, show_avg_cpm

    )

select *
from campaigns
