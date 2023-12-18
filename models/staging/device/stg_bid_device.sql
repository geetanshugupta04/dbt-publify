with

    bids as (

        select
            -- date(start_datetime at time zone 'Asia/Kolkata') as date,
            date(start_datetime) as date,
            ad_type,
            lower(make) as make,
            lower(model) as model,
            lower(device_os) as device_os,
            sum(count) as bid_requests,
            sum(case when bid_status = true then count else 0 end) as bid_response
        from publify_raw.bid_device_hourly_sep_22_30_csv
        where
            -- date(start_datetime at time zone 'Asia/Kolkata')
            -- between ('{start_date}') and ('{end_date}')
            -- and 
            country in ('IND', 'IN', 'in')
        group by
            -- date(start_datetime at time zone 'Asia/Kolkata'),
            date(start_datetime),
            ad_type,
            lower(make),
            lower(model),
            lower(device_os)
    )

select date, sum(bid_requests), sum(bid_response)
from bids
group by 1
