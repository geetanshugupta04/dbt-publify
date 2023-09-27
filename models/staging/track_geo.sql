with

    track as (
        select
            line_item_id,
            date,
            coalesce(pincode, 'NA') as pincode,
            app_id,
            sum(impression) as impression,
            sum(creative_view) as creative_view,
            sum(click) as click,
            sum(complete) as complete

        from publify_raw.track_geo_hourly
        group by 
            line_item_id,
            date,
            pincode,
            app_id

    )

select *
from track
