with

    track as (
        select
            line_item_id,
            date,
            ad_type,
            coalesce(pincode, 'NA') as pincode,
            app_id,
            ssp,
            sum(impression) as impression,
            sum(creative_view) as creative_view,
            sum(click) as click,
            sum(complete) as complete

        from publify_raw.track_geo_hourly_sep22_30_csv
        where
            -- date(start_datetime at time zone 'Asia/Kolkata')
            -- between ('{start_date}') and ('{end_date}')
            -- and 
            country in ('IND', 'IN', 'in')
        group by 
            line_item_id,
            date,
            pincode,
            app_id,
            ad_type,
            ssp

    )

select *
from track
