with

    track as (
        select
            line_item_id,
            date,
            coalesce(pincode, 'NA') as pincode,
            app_id,
            ssp,
            sum(impression) as impression,
            sum(creative_view) as creative_view,
            sum(click) as click,
            sum(complete) as complete

        from paytunes_data.track_geo_campaigns_csv
        group by 
            line_item_id,
            date,
            pincode,
            app_id,
            ssp

    )

select *
from track
