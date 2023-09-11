with

    vast as (
        select
            start_datetime,
            ad_type,
            make,
            model,
            device_os,
            count

        from vbids_device_hourly_csv

    )

select *
from vast
