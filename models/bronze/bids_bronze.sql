with

    bids as (
        select
            date(start_datetime) as date,
            ad_type,
            make,
            model,
            device_os,
            sum(count) as bid_requests,
            sum(case when bid_status = true then count else 0 end) as bid_response

        from bid_device_hourly_csv
        group by date, ad_type, make, model, device_os

    )

select *
from bids
