with

    vast as (
        select
            start_datetime,
            ad_type,
            pincode,
            count

        from publify_raw.vbids_geo_hourly

    )

select *
from vast
