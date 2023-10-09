with

    vast as (
        select
            date(start_datetime) as date,
            ad_type,
            coalesce(pincode, 'NA') as pincode,
            sum(count) as bid_wins
        from publify_raw.vbids_geo_hourly_sep_22_30_csv
        where country in ('IND', 'IN', 'in')
        group by date(start_datetime), ad_type, pincode

    )

select *
from vast
