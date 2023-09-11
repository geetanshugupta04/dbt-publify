with

    track as (
        select
            start_datetime,
            ad_type,
            make,
            model,
            device_os,
            impression,
            creative_view,
            click,
            complete

        from track_device_hourly_csv

    )

select *
from track
