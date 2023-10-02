with

    track as (
        select
            date(start_datetime at time zone 'Asia/Kolkata') as date,
            ad_type,
            make,
            model,
            device_os,
            sum(impression) as impression,
            sum(creative_view) as creative_view,
            sum(click) as clicks,
            sum(complete) as complete
        from publify_raw.track_device_hourly_sep_22_30_csv
        where
            -- date(start_datetime at time zone 'Asia/Kolkata')
            -- between ('{start_date}') and ('{end_date}')
            -- and 
            country in ('IND', 'IN', 'in')
        group by
            date(start_datetime at time zone 'Asia/Kolkata'),
            ad_type,
            make,
            model,
            device_os
    )

select *
from track
