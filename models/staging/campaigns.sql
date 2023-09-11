with
    campaigns as (
        select
            id as campaign_id,
            name as campaign_name,
            created_at as campaign_created_at,
            start_date,
            end_date,
            show_avg_cpm

        from campaign_campaign_1_csv

    )

select *
from campaigns
