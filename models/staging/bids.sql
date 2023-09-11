with

    bids as (
        select
            start_datetime,
            ad_type,
            make,
            model,
            device_os,
            count,
            bid_status

        from bid_device_hourly_csv

    )

select *
from bids
