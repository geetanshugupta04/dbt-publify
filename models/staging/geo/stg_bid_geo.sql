with

    bids as (
        select
            start_datetime,
            ad_type,
            pincode,
            bid_status,
            count

        from publify_raw.bid_geo_hourly_csv

    )

select *
from bids
