with

    vast as (
        select
            date(start_datetime) as date,
            ad_type,
            lower(make) as make,
            lower(model) as model,
            lower(device_os) as device_os,
            sum(count) as bid_wins
        from publify_raw.vbids_device_hourly_spe_22_30_csv
        where
            -- date(start_datetime at time zone 'Asia/Kolkata') between
            -- ('{start_date}')
            -- and ('{end_date}')
            -- and 
            country in ('IND', 'IN', 'in')
        group by
            date(start_datetime),
            ad_type,
            lower(make),
            lower(model),
            lower(device_os)
    )

select *
from vast
