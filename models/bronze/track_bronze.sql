with

    track as (
        select
            date(start_datetime) as date,
            ad_type,
            make,
            model,
            device_os,
            sum(impression) as delivered_impressions,
            sum(creative_view) as delivered_companion_impressions,
            sum(click) as clicks,
            sum(complete) as complete

        from {{ ref("track")}}
        group by date, make, ad_type, model, device_os

    )

select *
from track
