with
    campaigns as (
        select
            id as campaign_id,
            name as campaign_name,
            min(start_date) as campaign_start_date,
            max(end_date) as campaign_end_date,
            show_avg_cpm

        from publify_raw.campaign_campaign
        group by id, campaign_name, show_avg_cpm
    )

select *
from campaigns
