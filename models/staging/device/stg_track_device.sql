with

    track as (
        select
            date(start_datetime) as date,
            ad_type,
            line_item_id,
            app_id,

            lower(make) as make,
            lower(model) as model,
            lower(device_os) as device_os,

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
            date,
            ad_type,
            line_item_id,
            app_id,
            lower(make),
            lower(model),
            lower(device_os)
    )

select *
from track
